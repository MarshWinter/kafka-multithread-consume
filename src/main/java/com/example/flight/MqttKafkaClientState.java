package com.example.flight;

import com.example.internal.CommitOffset;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 客户端状态，用来处理相关状态管理，比如飞行窗口状态，任务提交状态，
 *
 *
 * @ClassName MqttKafkaClientState
 * @Author lianxiaodong
 * @Date 2022年09月08日 7:51 PM
 */
@Data
@Slf4j
public class MqttKafkaClientState {

    private static final int COMMIT_TIMES_THRESHOLD = 100;
    private static ConcurrentHashMap<String, MqttKafkaClientState> flyWay = new ConcurrentHashMap();

    /**
     * 最大飞行窗口，消息在完成 接收 - 执行 - 提交 这几步之后才会释放飞行窗口资源
     */
    private final int maxInFlight;

    private final Semaphore flightWindow;

    private final CommitBG commitBG;

    private AtomicInteger completeTimes = new AtomicInteger();

    /**
     * 用来控制提交偏移量等的超时时间,<br/>
     * @see org.apache.kafka.clients.consumer.internals.ConsumerCoordinator#timeToNextPoll(long)
     * @see org.apache.kafka.clients.consumer.internals.ConsumerCoordinator#pollHeartbeat(long)
     */
    private final long maxPollIntervalMs;

    /**
     * 公平锁，避免后面随着consumer使用线程数增加，导致有些线程抢不到锁，目前只有poll线程以及提交线程使用consumer
     */
    private Lock consumerLock = new ReentrantLock(true);

    /**
     * 提供弱一致性的飞行窗口计数
     */
    private AtomicInteger actualInConsuming = new AtomicInteger(0);

    /**
     * 保存待提交的偏移量
     */
    private ConcurrentHashMap<TopicPartition, LinkedBlockingQueue<CommitOffset>> waiteCommitOffset = new ConcurrentHashMap<>();
    /**
     * 保存 消费者
     * 按照目前客户端设计，采用单消费组，
     * 1. 同一个消费组不同分区可以重复提交
     * 2. 单一客户端只保存一个KafkaConsumer对象
     */
    private KafkaConsumer<String, String> kafkaConsumer;

    private MqttKafkaClientState(int maxInFlight, long maxPollIntervalMs) {
        this.maxInFlight = maxInFlight;
        this.flightWindow = new Semaphore(maxInFlight);
        this.commitBG = new CommitBG(this, maxInFlight, 1, 60, TimeUnit.SECONDS);
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public static MqttKafkaClientState getMqttKafkaClientState(int maxInFlight, long maxPollIntervalMs, String gruopId) {
        return flyWay.computeIfAbsent(gruopId, s -> {
            return new MqttKafkaClientState(maxInFlight, maxPollIntervalMs);
        });
    }

    public KafkaConsumer<String, String> getConsumer() {
        return kafkaConsumer;
    }


    // region 飞行窗口控制方法

    public void receiveMessage(CommitOffset commitOffset) throws InterruptedException {
        flightWindow.acquire();
        actualInConsuming.incrementAndGet();
        commitBG.pendingWork(commitOffset);
    }

    public void decreaseInFlight() {
        actualInConsuming.decrementAndGet();
        flightWindow.release();
    }

    public void decreaseInFlight(int num) {
        actualInConsuming.accumulateAndGet(num, (prev, x) -> prev - x);
        flightWindow.release(num);
    }

    /**
     * 标识偏移量已经可以提交
     * @param commitOffset 偏移以及分区数据
     */
    public void prepareCommit(CommitOffset commitOffset) {
        commitOffset.prepareCommit();
        if (completeTimes.updateAndGet(operand -> {
            operand++;
            if (operand % COMMIT_TIMES_THRESHOLD == 0) {
                operand = 0;
            }
            return operand;
        }) == 0) {
            this.commitBG.kickOffCommit();
        }
    }

    // endregion

}
