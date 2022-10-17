package com.example;

import com.alibaba.fastjson.JSON;
import com.example.flight.MqttKafkaClientState;
import com.example.flight.MqttKafkaMessageListenerWrapper;
import com.example.flight.RebalanceListener;
import com.example.internal.CommitOffset;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

/**
 * 客户端
 */
public class MqttKafkaClient {

    private static final Logger logger = LoggerFactory.getLogger(MqttKafkaClient.class);

    private static final int DEFAULT_SHUTDOWN_MS_TIME = 30_000;

    /**
     * 因为分别有两个关闭流程
     * <ul>
     *     <li>1）consumer关闭 1个关闭延迟</li>
     *     <li>---- 需要涉及到关闭执行线程池 0.5个关闭延迟</li>
     *     <li>2）提交线程关闭 1个关闭延迟</li>
     * </ul>
     */
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS_TIME = 5 * DEFAULT_SHUTDOWN_MS_TIME / 2;

    private Map<String, IMqttKafkaMessageListener> callbacks = new ConcurrentHashMap<>();

    private BrokerEnvEnum brokerEnv;

    /**
     * 状态锁，控制Mqtt客户端开启和关闭同步执行
     */
    private Object stateLock = new Object();

    /**
     * 消息拉取线程
     */
    private Thread pollKafkaMessageThread;

    // region 飞行窗口特性

    private boolean enableInFlight = false;

    private ExecutorService executorService;

    private MqttKafkaClientState mqttKafkaClientState = null;

    /**
     * 最大拉取时间
     */
    private int maxPollIntervalMs = 2 * DEFAULT_SHUTDOWN_MS_TIME;

    // endregion

    /**
     * 简易控制客户端状态的枚举
     */
    private enum State {INITIAL, STOPPED, STOPPING, RUNNING}

    private volatile State consumerCurrentState = State.INITIAL;


    // region 事件交互Consumer

    private Properties kafkaConsumerProperties;

    private KafkaConsumer<String, String> kafkaConsumer;

    private List<String> kafkaConsumerTopics = new ArrayList<>();

    // endregion

    /**
     * 消息订阅
     *
     * @param mqttTopic
     * @param mqttQos
     * @param messageListener
     */
    public void subscribe(String mqttTopic, int mqttQos, IMqttKafkaMessageListener messageListener) {
        if (!isRunning()) {
            throw new RuntimeException("MqttKafkaClient已断开连接");
        }
        // 如果是具备飞行窗口特性的客户端，使用包装类
        if (enableInFlight) {
            messageListener = new MqttKafkaMessageListenerWrapper(messageListener, mqttKafkaClientState);
        }
        this.callbacks.put(mqttTopic, messageListener);
        logger.info("发送订阅指令kafkaConsumerTopicSubscribe：{},mqttTopic:{}",  mqttTopic);
    }

    /**
     * 连接建立,<br/>
     * 建议全部订阅信息完成之后再建立连接
     */
    public void connect() {
        synchronized (stateLock) {
            logger.info("MqttKafkaClient starting...");
            if (this.consumerCurrentState != State.INITIAL) {
                throw new IllegalStateException("非初始化客户端，无法建立连接，建议重新new一个客户端进行使用！");
            }
            if (this.consumerCurrentState == State.INITIAL) {
                // 初始化线程
                this.consumerCurrentState = State.RUNNING;
                this.kafkaConsumer = new KafkaConsumer<>(this.kafkaConsumerProperties);
                initPollKafkaMessageThread(this.kafkaConsumer);
            }
            logger.info("MqttKafkaClient started");
        }
    }

    /**
     * 等待订阅信息完整
     */
    private void waitSubscribeInfo() {
        int previous = 0;
        while (previous != this.callbacks.size() || previous == 0) {
            previous = this.callbacks.size();
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                logger.error("异步订阅线程被中断，", e);
            }
            // 保证状态判断是同步的
            synchronized (stateLock) {
                if (!isConsumerRunning()) {
                    // 如果消费者不在运行中状态，则没有必要继续循环
                    logger.info("主题同步过程中客户端被终止...退出循环");
                    break;
                }
            }
        }
        logger.info("订阅信息已经完善，消费者线程开始消费");
    }

    /**
     * 连接断开
     */
    public void disconnect() {
        synchronized (stateLock) {
            logger.info("MqttKafkaClient stopping.");
            shutdownConsumer();
            // 关闭提交线程的执行
            this.mqttKafkaClientState.getCommitBG().stop(Duration.ofMillis(DEFAULT_SHUTDOWN_MS_TIME));
            logger.info("MqttKafkaClient stopped.");
        }
    }

    /**
     * 关闭消费者
     */
    public void shutdownConsumer() {
        synchronized (stateLock) {
            logger.info("consumer stopping...");
            if (!isConsumerRunning()) {
                logger.info("consumer has already been shutdown...");
                return;
            }
            this.consumerCurrentState = State.STOPPING;
            try {
                this.kafkaConsumer.wakeup();
                this.pollKafkaMessageThread.join(DEFAULT_SHUTDOWN_MS_TIME);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            } finally {
                logger.info("consumer stopp result is: pollKafkaMessageThread:{}", this.pollKafkaMessageThread.getState());
                if (this.pollKafkaMessageThread.getState() != Thread.State.TERMINATED) {
                    this.pollKafkaMessageThread.interrupt();
                }
                this.consumerCurrentState = State.STOPPED;
            }
        }
    }


    /**
     * 客户端初始化
     *
     * @param brokerEnv
     * @param kafkaGroupId
     */
    public MqttKafkaClient(BrokerEnvEnum brokerEnv, String kafkaGroupId) {
        initMqttKafkaClient(brokerEnv, kafkaGroupId, generateUuid(), generateUuid());
    }

    /**
     * 客户端初始化
     *
     * @param brokerEnv
     * @param kafkaGroupId
     * @param kafkaConsumerClientId
     * @param kafkaProducerClientId
     */
    public MqttKafkaClient(BrokerEnvEnum brokerEnv, String kafkaGroupId,
                           String kafkaConsumerClientId, String kafkaProducerClientId) {
        initMqttKafkaClient(brokerEnv, kafkaGroupId, kafkaConsumerClientId, kafkaProducerClientId);
    }

    /**
     * 客户端初始化,提供飞行窗口的可选特性<br/>
     * 注意：如果{@code enableInFilght}为true，则{@code maxInFlight}必须为非零整数，同时{@code executorService}线程服务必须非空（否则飞行窗口没有意义）<br/>
     * 考虑到{@link CommitOffset 提交标识}在线程间的传递
     * @param brokerEnv
     * @param kafkaGroupId
     * @param kafkaConsumerClientId
     * @param kafkaProducerClientId
     * @param enableInFlight
     * @param maxInFlight
     * @param executorService
     */
    public MqttKafkaClient(BrokerEnvEnum brokerEnv, String kafkaGroupId,
                           String kafkaConsumerClientId, String kafkaProducerClientId,
                           boolean enableInFlight,
                           int maxInFlight, ExecutorService executorService) {
        if (enableInFlight) {
            if ( maxInFlight < 0 || executorService == null) {
                // 参数合法性校验
                throw new IllegalArgumentException("");
            }
        }
        // 飞行窗口特性
        this.enableInFlight = enableInFlight;
        this.executorService = executorService;
        this.maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL_MS_TIME;
        this.mqttKafkaClientState = MqttKafkaClientState.getMqttKafkaClientState(maxInFlight, maxPollIntervalMs, kafkaGroupId);
        this.mqttKafkaClientState.getCommitBG().start("Commit-" + kafkaConsumerClientId + "-Thread");
        initMqttKafkaClient(brokerEnv, kafkaGroupId, kafkaConsumerClientId, kafkaProducerClientId);
    }

    public void initMqttKafkaClient(BrokerEnvEnum brokerEnv, String kafkaGroupId,
                                    String kafkaConsumerClientId, String kafkaProducerClientId) {
        paramCheck(brokerEnv, kafkaGroupId, kafkaConsumerClientId, kafkaProducerClientId);
        this.brokerEnv = brokerEnv;

        // ===（事件交互）订阅者配置信息===
        this.kafkaConsumerProperties = new Properties();
        this.kafkaConsumerProperties.put(CommonClientConfigs.CLIENT_ID_CONFIG, kafkaConsumerClientId);
        this.kafkaConsumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerEnv.getKafkaServer());
        this.kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        if (enableInFlight) {
            this.kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        } else {
            this.kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        }
        this.kafkaConsumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        this.kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.kafkaConsumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        // ** 重要，对于rebalance机制的影响很重要
        this.kafkaConsumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        this.kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    }

    private void paramCheck(BrokerEnvEnum brokerEnv, String groupId,
                            String kafkaConsumerClientId, String kafkaProducerClientId) {
        if (brokerEnv == null) {
            throw new RuntimeException("brokerEnv不允许为NULL");
        }
        if (isBlank(groupId)) {
            throw new RuntimeException("groupId不允许为空");
        }
        String[] split = groupId.split(":");
        if (split.length != 2) {
            throw new RuntimeException("groupId必须且只能包含一个':', groupId的命名规范为: {serverName}:{env}");
        }
        if (isBlank(kafkaConsumerClientId)) {
            throw new RuntimeException("kafkaConsumerClientId不允许为空");
        }
        if (isBlank(kafkaProducerClientId)) {
            throw new RuntimeException("kafkaProducerClientId不允许为空");
        }
    }


    private void initPollKafkaMessageThread(KafkaConsumer<String, String> kafkaConsumer) {
        kafkaConsumer.subscribe(kafkaConsumerTopics, new RebalanceListener(mqttKafkaClientState));

        ConsumerBG consumerThreadInitialBG = new ConsumerBG("initPollKafkaMessageThread-", this.enableInFlight, this.mqttKafkaClientState);
        consumerThreadInitialBG.start();
    }

    /**
     * 判断客户端是否存活
     *
     * @return
     */
    public boolean isRunning() {
        return isConsumerRunning();
    }

    /**
     * 判断kafka消费者是否存活
     *
     * @return
     */
    public boolean isConsumerRunning() {
        return this.consumerCurrentState == State.RUNNING;
    }

    private static Set<Integer> qosList = new HashSet<>(Arrays.asList(0, 1, 2));

    /**
     * mqttKafkaMessage参数检查
     *
     * @param mqttKafkaMessage
     */
    public static void checkMqttKafkaPublishMessage(MqttKafkaMessage mqttKafkaMessage) {

        if (!qosList.contains(mqttKafkaMessage.getQos()) || isBlank(mqttKafkaMessage.getPayload()) || isBlank(mqttKafkaMessage.getTopic())) {
            logger.error("消息发布参数异常,Qos:{}，topic:{}，mqttMsg:{}", mqttKafkaMessage.getQos(), mqttKafkaMessage.getTopic(),
                    mqttKafkaMessage.getPayload());
            throw new RuntimeException("mqtt消息发布参数异常");
        }

        if (isBlank(mqttKafkaMessage.getTraceId())) {
            // traceId为空，赋值
            mqttKafkaMessage.setTraceId(generateUuid());
        }

    }

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static String generateUuid() {
        String s = UUID.randomUUID().toString();
        return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18) + s.substring(19, 23) + s.substring(24);
    }

    public BrokerEnvEnum getBrokerEnv() {
        return brokerEnv;
    }

    public String getKafkaGroupId() {
        return (String) this.kafkaConsumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public String getKafkaConsumerClientId() {
        return (String) this.kafkaConsumerProperties.get(CommonClientConfigs.CLIENT_ID_CONFIG);
    }

    private class ConsumerBG implements Runnable {

        private final String threadNamePrefix;
        private final MqttKafkaClientState mqttKafkaClientState;
        private volatile String threadName;
        /**
         * 是否开启"飞行窗口"特性
         */
        private final boolean enableInFlight;

        public ConsumerBG(String threadNamePrefix, boolean enableInFlight, MqttKafkaClientState mqttKafkaClientState) {
            this.threadNamePrefix = threadNamePrefix;
            this.enableInFlight = enableInFlight;
            this.mqttKafkaClientState = mqttKafkaClientState;
        }

        @Override
        public void run() {
            waitSubscribeInfo();
            this.mqttKafkaClientState.setKafkaConsumer(kafkaConsumer);
            logger.info("消费者线程:{}, 开启", threadName);
            Set<TopicPartition> assignment = new HashSet<>();
            // seekToEnd之前必须先poll一下，用于获取partition的状态。直接seekToEnd的话由于缺少partition状态会报错。
            while (assignment.size() == 0) {
                kafkaConsumer.poll(Duration.ofMillis(100));
                assignment = kafkaConsumer.assignment();
            }
            kafkaConsumer.seekToEnd(assignment);
            kafkaConsumer.poll(Duration.ofMillis(100));
            kafkaConsumer.commitSync();

            try {
                while (isConsumerRunning()) {
                    try {
                        Lock consumerLock = mqttKafkaClientState.getConsumerLock();
                        ConsumerRecords<String, String> records = null;
                        consumerLock.lock();
                        try {
                            records = kafkaConsumer.poll(Duration.ofMillis(1000));
                        } finally {
                            consumerLock.unlock();
                        }

                        if (null == records || records.count() <= 0) {
                            continue;
                        }
                        // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        // long start = System.currentTimeMillis();
                        // logger.info("消息消费开始：{} ", sdf.format(new Date()));
                        for (ConsumerRecord<String, String> record : records) {
                            String kafkaMessage = record.value();
                            MqttKafkaMessage mqttKafkaSubscribeMessage = JSON.parseObject(kafkaMessage, MqttKafkaMessage.class);
                            IMqttKafkaMessageListener messageListener = MqttKafkaClient.this.callbacks.get(mqttKafkaSubscribeMessage.getTopic());

                            if (messageListener == null) {
                                continue;
                            }

                            // 获取对应的产品类型
                            String topic = record.topic();
                            String upMqttKafkaTopicPrefix = brokerEnv.getUpMqttKafkaTopicPrefix();
                            String productType = topic.substring(upMqttKafkaTopicPrefix.length());

                            if (enableInFlight) {
                                // 如果是具有飞行窗口的特性
                                int partition = record.partition();
                                TopicPartition topicPartition = new TopicPartition(topic, partition);
                                long offset = record.offset();
                                // 在接收线程中执行消息入队操作，保证消息的顺序性
                                final CommitOffset commitOffset = new CommitOffset(topicPartition, offset);
                                mqttKafkaClientState.receiveMessage(commitOffset);
                                final MqttKafkaMessageListenerWrapper messageListenerWrapper = (MqttKafkaMessageListenerWrapper) messageListener;
                                executorService.submit(()->{
                                    messageListenerWrapper.messageArrived(mqttKafkaSubscribeMessage, commitOffset);
                                });
                            } else {
                                // 执行对应的回调
                                messageListener.messageArrived(mqttKafkaSubscribeMessage);
                            }

                        }
                        // logger.info("消息消费结束：{} , 耗时: {}ms", sdf.format(new Date()) , System.currentTimeMillis() - start);
                    } catch (WakeupException e) {
                        logger.info("消息拉取消费者被唤醒");
                        // ignore ...
                        // 没有必要特别处理唤醒请求
                    } catch (org.apache.kafka.common.errors.InterruptException e) {
                        logger.warn("{}已终止，不再接收消息", threadName);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        logger.error("kafka消费失败, exception:{}", e.getMessage(), e);
                    }
                }
            } finally {
                // 自动提交，如果手动提交需要增加手动提交offset的代码
                if (enableInFlight) {
                    // 飞行窗口需要手动提交任务
                    // 考虑到滚动发布的语义，此处先不关闭consumer，在disconnect方法处执行
                } else {
                    kafkaConsumer.close();
                }
                logger.info("消费者线程:{}, 关闭", threadName);
            }
        }

        void start() {
            this.threadName = this.threadNamePrefix + System.currentTimeMillis();
            pollKafkaMessageThread = new Thread(this, this.threadName);
            pollKafkaMessageThread.start();
        }



    }




}
