package com.example.flight;

import com.example.IMqttKafkaMessageListener;
import com.example.MqttKafkaMessage;
import com.example.internal.CommitOffset;
import lombok.extern.slf4j.Slf4j;

/**
 * 用来做功能扩展的静态代理类
 * @ClassName MqttKafkaMessageListenerWrapper
 * @Author lianxiaodong
 * @Date 2022年09月08日 5:39 PM
 */
@Slf4j
public class MqttKafkaMessageListenerWrapper implements IMqttKafkaMessageListener {

    /**
     * 被代理的listener
     */
    private final IMqttKafkaMessageListener proxed;

    private final MqttKafkaClientState mqttKafkaClientState;


    public MqttKafkaMessageListenerWrapper(IMqttKafkaMessageListener mqttKafkaMessageListener, MqttKafkaClientState mqttKafkaClientState) {
        this.proxed = mqttKafkaMessageListener;
        this.mqttKafkaClientState = mqttKafkaClientState;
    }

    public void messageArrived(MqttKafkaMessage mqttKafkaMessage, CommitOffset commitOffset) {
        try {
            messageArrived(mqttKafkaMessage);
        } catch (Throwable throwable) {
            // todo 准备重试策略？？
            log.error("消息处理出错,", throwable);
        } finally {
            // 准备提交工作
            this.mqttKafkaClientState.prepareCommit(commitOffset);
        }

    }

    @Override
    public void messageArrived( MqttKafkaMessage mqttKafkaMessage) {
        this.proxed.messageArrived( mqttKafkaMessage);
    }
}
