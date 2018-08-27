package com.hdh.kafkaTest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author 华仔
 * @Description //TODO
 * by 华仔 创建.
 **/
public class MyKafkaProducer {

    /**
     * 写入到kafka的数据将写到磁盘并复制到集群中保证容错性。并允许生产者等待消息应答，直到消息完全写入。
     */
    private final Producer<String, String> producer;
    public final static String TOPIC = "my-replicated-topic";

    private MyKafkaProducer() {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("zk.connect", "127.0.0.1:2181");

        // 配置value的序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 配置key的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("acks", "all");

        producer = new KafkaProducer<String, String>(props);
    }

    void produce() {
        int messageNo = 100;
        final int COUNT = 200;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            // 生产者 发送topic 可以配置 topic主题、partition分区、key键、data值 、 timestamp 时间、 headers 头部
            producer.send(new ProducerRecord<String, String>(TOPIC, key, data));
            System.out.println(data);
            messageNo++;
        }
        producer.close();
    }

    public static void main(String[] args) {
        new MyKafkaProducer().produce();
    }



}
