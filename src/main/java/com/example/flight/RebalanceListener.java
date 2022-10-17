package com.example.flight;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;

/**
 *
 * @ClassName RebalanceListener
 * @Author lianxiaodong
 * @Date 2022年09月19日 5:28 PM
 */
@Slf4j
public class RebalanceListener implements ConsumerRebalanceListener {


    private final MqttKafkaClientState mqttKafkaClientState;

    public RebalanceListener(MqttKafkaClientState mqttKafkaClientState) {
        this.mqttKafkaClientState = mqttKafkaClientState;
    }

    /**
     * 如果在此处提交耗时过多的话，会发生rebalance, 导致{@link org.apache.kafka.clients.consumer.CommitFailedException}
     * @param partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.size() > 0) {
            long maxPollIntervalMs = this.mqttKafkaClientState.getMaxPollIntervalMs();
            Duration timeout = Duration.ofMillis(maxPollIntervalMs - 200 > 0 ? maxPollIntervalMs - 200 : 200);
            this.mqttKafkaClientState.getCommitBG().drainWorkAndCommit(Duration.ofMillis(maxPollIntervalMs));
        }
        log.info("旧的分区被回收: {}", partitions);

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("新的分区被分配: {}", partitions);
    }
}
