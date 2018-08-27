package com.hdh.kafkaTest;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * @author 华仔
 * @Description //TODO
 * by 华仔 创建.
 **/
public class MyKafkaConsumer3 {

    private final KafkaConsumer<String, String> consumer;

    private MyKafkaConsumer3() {
        Properties props = new Properties();
        // zookeeper 配置
        props.put("bootstrap.servers", "127.0.0.1:9092");

        /** group 代表一个消费组
         * 一个发布在Topic上消息被分发给此 消费者组 中的一个消费者。
         * 假如所有的消费者都在一个组中，那么这就变成了queue模型。
         * 假如所有的消费者都在不同的组中，那么就完全变成了发布-订阅模型。
         *
         * Topic分区中消息只能由消费者组中的唯一一个消费者处理，所以消息肯定是按照先后顺序进行处理的。
         * 但是它也仅仅是保证Topic的一个分区顺序处理，不能保证跨分区的消息先后处理顺序。 所以，如果你想要顺序的处理Topic的所有消息，那就只提供一个分区。
         */
        props.put("group.id", "test-group-2");

        // 自动提交
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        // latest, earliest, none
//        props.put("auto.offset.reset", "none");

        // zk
        props.put("zookeeper.connect", "127.0.0.1:2181");
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("rebalance.max.retries", "5");
        props.put("rebalance.backoff.ms", "1200");

        // 序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String,String>(props);
    }

    void consume() {

        consumer.subscribe(Arrays.asList(MyKafkaProducer.TOPIC),new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // 将偏移设置到最开始  消息获取 从开始读取消息
                consumer.seekToBeginning(collection);
                // 将偏移设置到最后    消息获取 从上次消息的最末尾开始读取
//                consumer.seekToEnd(collection);
            }
        });

        while (true) {
            /** 消息模型可以分为两种，队列和发布-订阅式。
             * 队列的处理方式是       一组消费者从服务器读取消息，一条消息只有其中的一个消费者来处理。
             * 发布-订阅模型的方式是  消息被广播给所有的消费者，接收到消息的消费者都可以处理此消息。
             */
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }

    }

    public static void main(String[] args) {
        new MyKafkaConsumer3().consume();
    }


}
