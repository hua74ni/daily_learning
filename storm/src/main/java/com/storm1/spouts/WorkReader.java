package com.storm1.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class WorkReader implements IRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean computed = false;
    private TopologyContext context;

    @Override
    public boolean isDistributed() {
        return false;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.context = topologyContext;
        this.collector = spoutOutputCollector;
        try {
            this.fileReader = new FileReader(map.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+map.get("wordFile")+"]");
        }


    }

    @Override
    public void close() {}

    /**
     * 这个方法做的惟一一件事情就是分发文件中的文本行
     */
    @Override
    public void nextTuple() {
        /**
         * 这个方法会不断的被调用，直到整个文件都读完了，我们将等待并返回。
         */
        if (computed){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        String str;

        BufferedReader bufferedReader = new BufferedReader(fileReader);
        try{

            while((str = bufferedReader.readLine()) != null){
                /**
                 * 按行发布一个新值
                 */
                this.collector.emit(new Values(str));
            }

        }catch (Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }finally {
            computed = true;
        }

    }

    @Override
    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    /**
     * 声明输入域"word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("line"));

    }
}
