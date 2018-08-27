package com;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.bolt.SenqueceBolt;
import com.scheme.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class StormKafkaTopo {

    public static void main(String[] args) {

        // 配置Zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("127.0.0.1:2181");

        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"topic","","kafkaspout");

        // 配置KafkaBolt中的 kafka.broker.properties
        Config config = new Config();
        Map<String,String> map = new HashMap<String, String>();

        // 配置Kafka broker地址
        map.put("metadata.broker.list", "127.0.0.1:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put("kafka.broker.properties", map);
        config.put("topic", "topic2");

        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
        builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");

        if(args != null && args.length > 0){
            //提交到集群运行
            try{
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            }catch (Exception e){
                e.printStackTrace();
            }
        } else {
            // 本地模式运行
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Topotest1121", config, builder.createTopology());
            Utils.sleep(1000000);
            localCluster.killTopology("Topotest1121");
            localCluster.shutdown();
        }


    }

}
