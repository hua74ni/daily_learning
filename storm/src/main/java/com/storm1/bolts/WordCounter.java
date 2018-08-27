package com.storm1.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class WordCounter implements IRichBolt {

    Integer id;
    String name;
    Map<String,Integer> counters;
    private OutputCollector collector;

    private static final String SIGNALS = "signals";
    private static final String ACTION = "refreshCache";


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<String, Integer>();
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String str = null;

        try{
            str = tuple.getStringByField("word");
        }catch (IllegalArgumentException e) {
            //Do nothing
        }

        if(str != null){

            if(!this.counters.containsKey(str)){
                counters.put(str,1);
            }else{
                Integer c = counters.get(str);
                counters.put(str,c + 1);
            }

        }
        // 全部数据流组 清理计算器操作
//        else {
//
//            if((SIGNALS).equals(tuple.getSourceGlobalStreamid().get_streamId())){
//                str =  tuple.getStringByField("action");
//                if(ACTION.equals(str)){
//                    counters.clear();
//                }
//            }
//
//        }

        //对元组作为应答
        collector.ack(tuple);
    }

    /**
     * 通常情况下，当拓扑关闭时，你应当使用 cleanup() 方法关闭活动的连接和其它资源。
     */
    @Override
    public void cleanup() {
        System.out.println("-- 单词数 【" + name + "-" + id +"】 --");
        for(Map.Entry<String,Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }
}
