package banktransactions;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class TransactionsSpouts extends BaseRichSpout {

    static Logger LOG = Logger.getLogger(TransactionsSpouts.class);

    // 同个元组最大错误次数
    private static final Integer MAX_FAILS = 2;
    // 存放所有消息元组
    Map<Integer,String> messages;
    // 存放每个元组消息发送错误的次数
    Map<Integer,Integer> transactionFailureCount;
    // 待发送消息的元组
    Map<Integer,String> toSend;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        Random random = new Random();
        this.messages = new HashMap<Integer, String>();
        this.transactionFailureCount = new HashMap<Integer, Integer>();
        this.toSend = new HashMap<Integer, String>();

        for(int i = 0; i< 100; i++){
            messages.put(i, "transaction_"+random.nextInt());
            transactionFailureCount.put(i, 0);
        }
        toSend.putAll(messages);
        this.collector = spoutOutputCollector;

    }

    @Override
    public void close() {

    }

    @Override
    public void nextTuple() {

        if(!toSend.isEmpty()){
            for (Map.Entry<Integer,String> transactionEntry  : toSend.entrySet()){

                Integer transactionId = transactionEntry.getKey();
                String transactionMessage = transactionEntry.getValue();
                collector.emit(new Values(transactionMessage),transactionId);
            }
            toSend.clear();
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * spout发送消息 得到应答
     * 在存放消息元组中 删除对应应答的消息msgId
     */
    @Override
    public void ack(Object msgId) {

        messages.remove(msgId);
        LOG.info("Message fully processed ["+msgId+"]");

    }

    /**
     * spout发送消息 得到失败
     * 如果同一个消息元组发送失败次数2次 即停止storm
     * 将发送失败的消息元组重新发送，从message中获取对应数据 存放于toSend中，等待重新发送
     */
    @Override
    public void fail(Object msgId) {

        if(!transactionFailureCount.containsKey(msgId)){
            throw new RuntimeException("Error, transaction id not found ["+msgId+"]");
        }
        Integer transactionId = (Integer) msgId;

        //Get the transactions fail
        Integer failures = transactionFailureCount.get(transactionId) + 1;
        if(failures >= MAX_FAILS){
            throw new RuntimeException("Error, transaction id ["+transactionId+"] has had many errors ["+failures+"]");
        }

        transactionFailureCount.put(transactionId, failures);
        toSend.put(transactionId,messages.get(transactionId));
        LOG.info("Re-sending message ["+msgId+"]");

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionMessage"));
    }


}
