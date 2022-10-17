package com.example;

import java.util.HashMap;
import java.util.Map;

/**
 * kafka所有环境配置
 *
 */
public enum BrokerEnvEnum {

    ;

    /**
     * ssl连接
     */
    private String sslBrokerServer;
    /**
     * kafka-broker连接
     */
    private String kafkaServer;
    /**
     * 下行消息kafkaTopic
     */
    private String downMqttKafkaTopic;
    /**
     * 上行消息初次订阅kafkaTopic
     */
    private String upMqttSubscribeKafkaTopic;
    /**
     * 上行消息kafkaTopic前缀
     */
    private String upMqttKafkaTopicPrefix;
    /**
     * 上行实时消息kafkaTopic前缀
     */
    private String upRealtimeMqttKafkaTopicPrefix;

    /**
     * key: BrokerEnvEnum.name()
     * value: BrokerEnvEnum
     */
    private static Map<String, BrokerEnvEnum> nameMap;

    BrokerEnvEnum(String sslBrokerServer, String kafkaServer, String downMqttKafkaTopic, String upMqttSubscribeKafkaTopic, String upMqttKafkaTopicPrefix, String upRealtimeMqttKafkaTopicPrefix) {
        this.sslBrokerServer = sslBrokerServer;
        this.kafkaServer = kafkaServer;
        this.downMqttKafkaTopic = downMqttKafkaTopic;
        this.upMqttSubscribeKafkaTopic = upMqttSubscribeKafkaTopic;
        this.upMqttKafkaTopicPrefix = upMqttKafkaTopicPrefix;
        this.upRealtimeMqttKafkaTopicPrefix = upRealtimeMqttKafkaTopicPrefix;
    }

    static {
        BrokerEnvEnum[] values = values();
        nameMap = new HashMap<>(values.length);
        for (BrokerEnvEnum value : values) {
            nameMap.put(value.name(), value);
        }
    }

    public static BrokerEnvEnum parseByName(String enumName) {
        return nameMap.get(enumName);
    }

    public String getSslBrokerServer() {
        return sslBrokerServer;
    }

    public String getKafkaServer() {
        return kafkaServer;
    }

    public String getDownMqttKafkaTopic() {
        return downMqttKafkaTopic;
    }

    public String getUpMqttSubscribeKafkaTopic() {
        return upMqttSubscribeKafkaTopic;
    }

    public String getUpMqttKafkaTopicPrefix() {
        return upMqttKafkaTopicPrefix;
    }

    public String getUpRealtimeMqttKafkaTopicPrefix() {
        return upRealtimeMqttKafkaTopicPrefix;
    }

}
