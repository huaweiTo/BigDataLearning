package com.imooc.spark.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer extends Thread {
    private String topic;
    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROCKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        producer = new Producer<Integer, String>(new ProducerConfig(properties));

    }

    @Override   //重写run方法来发消息
    public void run() {
        int messageNo = 1;
        while (true) {     //不断地发消息
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent_" + message);
            messageNo++;
            try {
                Thread.sleep(2000);//睡2秒
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
