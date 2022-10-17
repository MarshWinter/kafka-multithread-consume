package com.example;

public interface IMqttKafkaMessageListener {

    void messageArrived(MqttKafkaMessage mqttKafkaMessage);

}
