package banktransactions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;
import java.util.Random;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class RandomFailureBolt extends BaseRichBolt {

    private static final Integer MAX_PERCENT_FAIL = 80;
    Random random = new Random  ();
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {

        // 模拟 百分之80的 消息发送元组发送失败
        // 百分之20的消息元组发送成功
        Integer r = random.nextInt(100);
        if(r > MAX_PERCENT_FAIL){
            collector.ack(input);
        }else{
            collector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

}
