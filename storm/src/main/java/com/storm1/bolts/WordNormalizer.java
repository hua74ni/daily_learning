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
public class WordNormalizer implements IRichBolt {

    private OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

    }

    /**
     * *bolt*从单词文件接收到文本行，并标准化它。
     * 文本行会全部转化成小写，并切分它，从中得到所有单词。
     */
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
                 * 调用中发布0~N元组
                 * 如果这个方法在一次调用中接收到句子 “This is the Storm book”，它将会发布五个元组。
                 */
                collector.emit(new Values(word));
            }
        }

        /**
         * 收到 spout的分配的任务
         * 对元组做出应答
         */
        collector.ack(tuple);

    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        // 声明输出域
        outputFieldsDeclarer.declare(new Fields("word"));

    }

}
