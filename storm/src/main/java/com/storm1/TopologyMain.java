package com.storm1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.storm1.bolts.WordCounter;
import com.storm1.bolts.WordNormalizer;
import com.storm1.groups.ModuleGrouping;
import com.storm1.spouts.SignalsSpout;
import com.storm1.spouts.WorkReader;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        // 定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WorkReader());

        // 全部数据流组
        builder.setSpout("signals-spout", new SignalsSpout());
        /** 随机数据流组指定数据源
         * shuffleGrouping 随机消费 来自 word-reader的数据
         *
         * note: 每个 InputDeclarer 可以有一个以上的数据源，而且每个数据源可以分到不同的组。
         */
        builder.setBolt("word-normalizer",new WordNormalizer())
                // 自定义数据流组
                .customGrouping("word-reader" , new ModuleGrouping());
//                .shuffleGrouping("word-reader");
        /** 域数据流组
         * 域数据流组允许你基于元组的一个或多个域控制如何把元组发送给 com.storm1.banktransactions。
         * 单词计数器的例子，如果你用 word 域为数据流分组，
         * word-normalizer bolt 将只会把相同单词的元组发送给同一个 word-counterbolt 实例。
         * fieldsGrouping 声明了 "word"的控制域
         */
        builder.setBolt("word-counter",new WordCounter(),2)
                .fieldsGrouping("word-normalizer",new Fields("word"));
        // 全部数据流组
//                .allGrouping("signals-spout" , "signals");
//        builder.setBolt("word-counter",new WordCounter(),2).shuffleGrouping("word-normalizer");

        //配置
        Config config = new Config();
//        config.put("wordsFile",args[0]);
        config.put("wordsFile","src/main/resources/words.txt");
        config.setDebug(false);

        //运行拓扑
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-started-Topologie", config, builder.createTopology());
        Thread.sleep(2000);
        cluster.shutdown();

    }

}
