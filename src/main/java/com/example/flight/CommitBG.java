package com.example.flight;

import com.example.internal.CommitOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * 提交的后台线程
 *
 * @ClassName CommitBG
 * @Author lianxiaodong
 * @Date 2022年09月14日 3:35 PM
 */
@Slf4j
public class CommitBG implements Runnable {

    private String threadName;

    /**
     * 提交间隔
     */
    private final long commitInterval;

    /**
     * 同步提交的超时配置
     *
     * @see #drainWorkAndCommit(Duration)
     * @see #commitSync(Map, long)
     */
    private long commitTimeout;

    /**
     * 执行超时时间
     */
    private final long executionTimeoutInMS;

    /**
     * 批量提交大小
     */
    private final int batchSize;

    private enum State {STARTING, RUNNING, COMMITING, STOPPING, STOPPED};
    private volatile State currentState = State.STOPPED;
    private Object lifecycle = new Object();

    private volatile boolean closed = false;

    /**
     * 暂停标识，主流程停止提交，由外部线程主动控制提交流程
     *
     * @see #drainWorkAndCommit(Duration)
     */
    private volatile boolean pause = false;



    /**
     * 保证外界触发提交操作和本身周期提交操作互斥，保证内存提交偏移的一致。
     * 目前只有revoke阶段会触发此操作，发生概率理论上应该很小，不应该导致很高的性能损耗
     */
    private Object commitLock = new Object();



    private final MqttKafkaClientState mqttKafkaClientState;

    private Thread commitThread = null;

    /**
     * 目前所有分区的提交任务放在一个队列里面，后期是否考虑为了增加并发度，考虑
     * 1）每个分区对应一个提交线程
     * 2）单线程管理多个分区
     */
    private LinkedBlockingDeque<CommitOffset> pendingWork = new LinkedBlockingDeque<>();

    /**
     * 提交任务触发器
     */
    private LinkedBlockingQueue<State> trigger = new LinkedBlockingQueue<>();

    /**
     * 提交中的偏移量，按照分区排列
     */
    private ConcurrentHashMap<TopicPartition, CommitOffset> inCommitting = new ConcurrentHashMap<>();

    /**
     * 记录上一次偏移提交状况,
     */
    private ConcurrentHashMap<TopicPartition, CommitOffset> lastCommitting = new ConcurrentHashMap<>();

    public CommitBG(MqttKafkaClientState mqttKafkaClientState, int batchSize, long commitInterval, long executionTimeout, TimeUnit timeUnit) {
        this.mqttKafkaClientState = mqttKafkaClientState;
        this.commitInterval = timeUnit.toMillis(commitInterval);
        this.batchSize = batchSize;
        this.executionTimeoutInMS = timeUnit.toMillis(executionTimeout);
        this.commitTimeout = this.commitInterval >> 1 > 200 ? this.commitInterval : 200;
    }

    @Override
    public void run() {
        synchronized (lifecycle) {
            currentState = State.RUNNING;
        }

        // 项目启动后延迟2s再执行具体逻辑
        try {
            TimeUnit.MILLISECONDS.sleep(2000 - System.currentTimeMillis() % 1000);
        } catch (InterruptedException e) {
            if (!isRunning()) {
                log.error(e.getMessage(), e);
            }
        }
        log.info(">>>>>>>>> {} thread start", threadName);
        try {
            while (isRunning()) {
                // 外部线程需要执行
                if (pause) {
                    try {
                        Thread.sleep(commitInterval);
                    } catch (InterruptedException e) {
                    }
                    continue;
                }


                try {
                    // 外部线程竞用锁
                    synchronized (commitLock) {
                        // 双重锁检测
                        if (pause) {
                            continue;
                        }

                        // 任务触发等待机制
                        if(this.mqttKafkaClientState.getFlightWindow().availablePermits() < 0.8*this.mqttKafkaClientState.getMaxInFlight() ) {
                            trigger.poll(commitInterval, TimeUnit.MILLISECONDS);
                            trigger.drainTo(new HashSet<>());
                        }
                        // 超过一定时限,直接执行下一次提交任务
                        long deadline = System.currentTimeMillis() + commitInterval;
                        // log.info("触发提交线程, 信号量数量:{}, 累积工作数量:{}, 第一个工作任务:{}",mqttKafkaClientState.getFlightWindow().availablePermits(), pendingWork.size(), pendingWork.peek());
                        doWorkAsync(deadline);
                    }

                } catch (InterruptedException e) {
                    if (isRunning()) {
                        log.error("提交被中断：{}", e.getMessage(), e);
                    }
                } catch (Exception e) {
                    if (isRunning()) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        } finally {
            synchronized (lifecycle) {
                currentState = State.STOPPED;
            }
        }
        log.info(">>>>>>>>> {} thread stop", threadName);

    }

    /**
     * 异步执行提交任务，由于涉及到相关相关成员状态变量进行修改，所以调用到的相关的地方需要利用{@link #commitLock}锁保护
     *
     * @param deadline
     * @see #doWorkSync(long)
     */
    private void doWorkAsync(long deadline) {
        Map<TopicPartition, CommitOffset> batchCommit = new HashMap<>();
        // log.info("开始deadline : {} ", deadline - System.currentTimeMillis());
        // 批量获取offset提交请求，最大不超过batchSize
        for (int i = 0;
             i < batchSize
                     && System.currentTimeMillis() < deadline;
             i++) {
            CommitOffset waitCommit = pendingWork.peek();
            // log.info("waitCommit : {}" , waitCommit);
            if (waitCommit == null) {
                break;
            }
            if (waitCommit.isInitial() && waitCommit.maybeInLock(executionTimeoutInMS)) {
                // log.info("进入熔断");
                // 熔断机制
                waitCommit = pendingWork.poll();
                // todo 可以考虑增加错误重试机制
                log.warn("任务:{}, 执行超时, 移出提交队列， 防止阻塞", waitCommit);
            } else if (waitCommit.isPrepareCommitting()) {
                // log.info("准备提交");
                // 放入待提交队列
                waitCommit = pendingWork.poll();
                waitCommit.committing();
                batchCommit.put(waitCommit.getTopicPartition(), waitCommit);
            } else if(waitCommit.isCommittable()){
                // log.info("可提交");
                pendingWork.poll();
            } else {
                // log.info("还不能提交");
                break;
            }
            // 同时增加飞行窗口资源
            this.mqttKafkaClientState.decreaseInFlight();
        }
        // log.info("结束deadline : {} ", deadline - System.currentTimeMillis());
        // log.info("提交线程遍历结束, 信号量数量:{}, 累积工作数量:{}, 第一个工作任务:{}",mqttKafkaClientState.getFlightWindow().availablePermits(), pendingWork.size(), pendingWork.peek());

        // 执行异步提交流程
        if (batchCommit.size() > 0) {
            // 有提交任务
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            batchCommit.entrySet().stream().forEach(topicPartitionCommitOffsetEntry -> {
                TopicPartition tp = topicPartitionCommitOffsetEntry.getKey();
                CommitOffset co = topicPartitionCommitOffsetEntry.getValue();
                offsets.put(tp, new OffsetAndMetadata(co.getOffset()));
                // 记录最新的提交
                this.inCommitting.put(tp, co);
            });
            // todo 可以考虑将窗口计数放入到异步回调函数里面，不过也可以认为是异步，效率优先，由最新的提交去覆盖，同时在关闭时利用同步提交来兜底
            commitAsync(offsets);

            long remain = deadline - System.currentTimeMillis();
            if (remain > 0 && this.mqttKafkaClientState.getFlightWindow().availablePermits() < 0.8 * this.mqttKafkaClientState.getMaxInFlight()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(remain);
                } catch (InterruptedException e) {
                }
            }

        }
    }

    /**
     * 同步执行提交任务，由于涉及到相关相关成员状态变量进行修改，所以调用到的相关的地方需要利用{@link #commitLock}锁保护
     *
     * @param deadline
     * @see #doWorkAsync(long)
     */
    private void doWorkSync(long deadline) {
        Map<TopicPartition, CommitOffset> batchCommit = new HashMap<>();
        CommitOffset waitCommit;
        int resourceCount = 0;
        // 批量获取offset提交请求，最大不超过batchSize
        for (int i = 0;
             i < batchSize
                     && System.currentTimeMillis() < deadline;
             i++) {
            waitCommit = pendingWork.peek();
            if (waitCommit == null) {
                break;
            }
            if (!waitCommit.isPrepareCommitting() && waitCommit.maybeInLock(executionTimeoutInMS)) {
                // 熔断机制
                waitCommit = pendingWork.poll();
                // todo 可以考虑增加错误重试机制
                log.warn("任务:{}, 执行超时, 移出提交队列， 防止阻塞", waitCommit);
            } else if (waitCommit.isPrepareCommitting()) {
                // 放入待提交队列
                waitCommit = pendingWork.poll();
                waitCommit.committing();
                batchCommit.put(waitCommit.getTopicPartition(), waitCommit);
            } else if(waitCommit.isCommitting()) {
                // 如果出现提交中状态，说明可能是异步提交结果尚未返回，
                // 侧面也反映了本来就应该提交，安全起见，重新提交一遍
                waitCommit = pendingWork.poll();
                batchCommit.put(waitCommit.getTopicPartition(), waitCommit);
            } else if (waitCommit.isCommitted()) {
                pendingWork.poll();
            } else {
                // 任务还未完成，直接跳出循环
                break;
            }
            resourceCount++;

        }

        // 执行同步提交流程
        if (batchCommit.size() > 0) {
            try {
                // 有提交任务
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                batchCommit.entrySet().stream().forEach(topicPartitionCommitOffsetEntry -> {
                    TopicPartition tp = topicPartitionCommitOffsetEntry.getKey();
                    CommitOffset co = topicPartitionCommitOffsetEntry.getValue();
                    offsets.put(tp, new OffsetAndMetadata(co.getOffset()));
                    // 记录最新的提交
                    this.inCommitting.put(tp, co);
                });
                commitSync(offsets, deadline);
            } finally {
                // 同时增加飞行窗口资源
                this.mqttKafkaClientState.decreaseInFlight(resourceCount);
            }
        }
    }


    /**
     * 保证consumer使用是在单线程内执行
     *
     * @param offsets 需要提交的偏移量
     */
    private void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 保证kafkaConsumer是在单线程执行
        KafkaConsumer<String, String> kafkaConsumer = mqttKafkaClientState.getKafkaConsumer();
        Lock consumerLock = mqttKafkaClientState.getConsumerLock();
        consumerLock.lock();
        try {
            kafkaConsumer.commitAsync(offsets, (tpOmMap, exception) -> {
                if (exception != null) {
                    log.error("异步提交失败: {}", tpOmMap, exception);
                } else {
                    log.debug("异步提交成功: {}", tpOmMap);
                }

                tpOmMap.entrySet().stream().forEach(topicPartitionOffsetAndMetadataEntry -> {
                    TopicPartition tp = topicPartitionOffsetAndMetadataEntry.getKey();
                    OffsetAndMetadata om = topicPartitionOffsetAndMetadataEntry.getValue();
                    recordCommitState(tp, om.offset(), exception);
                });
            });
        } finally {
            consumerLock.unlock();
        }
    }

    /**
     * 同步提交
     *
     * @param offsets
     * @param deadline
     * @return
     */
    private boolean commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, long deadline) {
        // 保证kafkaConsumer是在单线程执行
        KafkaConsumer<String, String> kafkaConsumer = mqttKafkaClientState.getKafkaConsumer();
        Lock consumerLock = mqttKafkaClientState.getConsumerLock();
        consumerLock.lock();
        boolean success = false;
        Exception exception = null;
        // 同步提交需要一定执行时间（网络）
        long timeoutMs = deadline - System.currentTimeMillis();
        Duration timeout = Duration.ofMillis(timeoutMs <0? commitTimeout: timeoutMs);
        try {
            kafkaConsumer.commitSync(offsets, timeout);
            success = true;
            log.debug("同步提交成功: {}", offsets);
        } catch (Exception e) {
            exception = e;
            log.error("同步提交失败: {}", offsets, e);
        } finally {
            final Exception e = exception;
            offsets.forEach((topicPartition, offsetAndMetadata) -> {
                recordCommitState(topicPartition, offsetAndMetadata.offset(), e);
            });
            consumerLock.unlock();
        }
        return success;
    }

    /**
     * 记录提交状态
     *
     * @param tp
     * @param offset
     * @param exception
     */
    public void recordCommitState(TopicPartition tp, long offset, Exception exception) {
        // 原子性保证
        CommitOffset committed = inCommitting.computeIfPresent(tp, (topicPartition, commitOffset) -> {
            if (commitOffset.getOffset() == offset) {
                // 按照KafkaConsumer的API文档说明，提交由api保证顺序，只需要保证最新的提交成功就行了
                commitOffset.committed(exception);
            }
            return commitOffset;
        });
        lastCommitting.compute(tp, (topicPartition, co) -> {
            if (co == null || committed.getOffset() >= co.getOffset()) {
                return committed;
            }
            if (co != null && committed.getOffset() < co.getOffset()) {
                // 理论上不应该出现这种情况
                log.error("提交了过去的偏移量，old: {}, new: {}", co, committed);
            }
            return null;
        });
    }

    /**
     * 做最后的清理工作,
     * 注意：在做清理工作时，使用的是同步提交方式
     */
    public void finalCheckAndClean(long deadline) {
        synchronized (commitLock) {
            // 目前策略是只提交已经正常完成的消息，如果有新想法，可以在此处修改
            if (this.pendingWork.size() > 0) {
                // 如果还有正在进行的任务，则打印错误日志，可能会导致重复消费
                LinkedList<CommitOffset> executing = new LinkedList<>();
                pendingWork.drainTo(executing);
                log.error("还有任务正在执行中，可能会导致重复消费，size: {}, data: {}", pendingWork.size(), executing);
                // 释放占用资源
                this.mqttKafkaClientState.decreaseInFlight(executing.size());
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            if (inCommitting.size() > 0) {
                for (Map.Entry<TopicPartition, CommitOffset> tpCo : inCommitting.entrySet()) {
                    TopicPartition tp = tpCo.getKey();
                    CommitOffset co = tpCo.getValue();
                    if (!co.isCommittedSuccessfully()) {
                        offsets.put(tp, new OffsetAndMetadata(co.getOffset()));
                    }
                }
                if (offsets.size() > 0) {
                    /**
                     * 可以考虑去除， 参考 {@link #drainWorkAndCommit(Duration)}
                     */
                    // 重试最后一次提交
                    commitSync(offsets, deadline);
                }
            }
            // 清理
            inCommitting.clear();
            log.info("最后一次分区提交状态:{}", lastCommitting);
            lastCommitting.clear();
        }
    }

    /**
     * 准备提交，加入到执行队列里面去
     *
     * @param commitOffset
     */
    public void pendingWork(CommitOffset commitOffset) {
        while (!pendingWork.offer(commitOffset)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        // log.info("加入待提交队列:{}, 任务队列大小:{}", commitOffset, pendingWork.size());
    }

    /**
     * 开始提交流程
     */
    public void kickOffCommit() {
        while (!trigger.offer(currentState)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * 提取所有任务并提交,
     * 最少需要两个 commitTimeout 时间，保证网络运行的时间
     */
    public void drainWorkAndCommit(Duration timeout) {
        pause = true;
        long start = System.currentTimeMillis();
        // 需要至少两个提交超时时间
        long timeoutInMs = Math.max(timeout.toMillis(), 2 * commitTimeout);
        // 需要2个提交超时时间，控制总体超时符合外部预期
        long deadline = start + timeoutInMs - 2 * commitTimeout;
        try {
            if (this.mqttKafkaClientState.getActualInConsuming().get() > 0) {
                log.warn("还有任务正在消费中，kafka发生rebalance");
            }
            synchronized (commitLock) {
                do {
                    // 在超时时间充裕的情况下，保证多次提交，尽最大可能将offset提交上去
                    long stepDeadline = start + commitTimeout;
                    doWorkSync(stepDeadline);
                } while (this.mqttKafkaClientState.getActualInConsuming().get() > 0 && System.currentTimeMillis() > deadline);
            }
            finalCheckAndClean(deadline);
        } finally {
            pause = false;
        }
    }

    // region 生命周期调度方法
    public void start(String threadName) {
        if (this.closed) {
            throw new IllegalStateException("这个提交后台线程已经关闭");
        }
        this.threadName = threadName;
        synchronized (lifecycle) {
            currentState = State.STARTING;
        }
        commitThread = new Thread(this, threadName);
        commitThread.start();
    }



    public void stop(Duration timeout) {
        if (closed) {
            return;
        }
        log.info(">>>>>>>>>>> {} stop begin...", threadName);
        closed = true;
        synchronized (lifecycle) {
            this.currentState = State.STOPPING;
        }
        drainWorkAndCommit(timeout);
        log.info(">>>>>>>>>>> {} stop end...", threadName);
    }


    /**
     * 是否正在运行中
     *
     * @return
     */
    public boolean isRunning() {
        synchronized (lifecycle) {
            return currentState != State.STOPPED && currentState != State.STOPPING;
        }
    }

    public boolean isCommitting() {
        synchronized (lifecycle) {
            return currentState == State.COMMITING;
        }
    }

    public boolean isStopping() {
        synchronized (lifecycle) {
            return currentState != State.STOPPING;
        }
    }
    // endregion




}
