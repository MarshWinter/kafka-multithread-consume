package com.example;

/**
 * Description: MqttKafkaMessage
 */
public class MqttKafkaMessage {

    /**
     * mqtt topic
     */
    private String topic;
    /**
     * mqtt消息内容
     */
    private String payload;
    /**
     * mqtt服务质量
     */
    private int qos;
    /**
     * 日志追踪用
     */
    private String traceId;
    /**
     * 终端编号；机器人编号，电梯编号，滑板车编号等
     */
    private String clientId;
    /**
     * 消息来源（车辆或设备），服务订阅kafka消息，解析数据时使用
     */
    private String from;

    public MqttKafkaMessage() {

    }

    public MqttKafkaMessage(String mqttTopic, String mqttMsg, int qos, String thingId) {
        this.topic = mqttTopic;
        this.payload = mqttMsg;
        this.qos = qos;
        this.clientId = thingId;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }
}
