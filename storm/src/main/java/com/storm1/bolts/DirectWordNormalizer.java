package com.storm1.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class DirectWordNormalizer implements IRichBolt {

    private OutputCollector collector;
    private Integer numCounterTasks;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.numCounterTasks = topologyContext.getComponentTasks("word-counter").size();

    }

    @Override
    public void execute(Tuple tuple) {

        String sentence = tuple.getString(0);
        String [] words = sentence.split(" ");
        for (String word:
                words) {
            word = word.trim();
            if(!word.isEmpty()){
                //发布这个单词
                word = word.toLowerCase();
                /**
                 * 直接数据流组
                 */
                collector.emitDirect(getWordCountIndex(word),new Values(word));
            }
        }

        /**
         * 收到 spout的分配的任务
         * 对元组做出应答
         */
        collector.ack(tuple);

    }

    public Integer getWordCountIndex(String word) {
        word = word.trim().toUpperCase();
        if(word.isEmpty()){
            return 0;
        }else{
            return word.charAt(0) % numCounterTasks;
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        // 声明输出域
        outputFieldsDeclarer.declare(new Fields("word"));

    }

}
