package com.storm1.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class SignalsSpout implements IRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;

    }

    @Override
    public void close() {

    }

    @Override
    public void nextTuple() {

        collector.emit("signals",new Values("refreshCache"));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream("signals",new Fields("action"));

    }
}
