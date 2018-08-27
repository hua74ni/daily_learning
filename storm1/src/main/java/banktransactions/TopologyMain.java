package banktransactions;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("transactions-spout",new TransactionsSpouts());
        topologyBuilder.setBolt("random-failure-bolt",new RandomFailureBolt())
                .shuffleGrouping("transactions-spout");

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(true);
        cluster.submitTopology("transactions-test",config,topologyBuilder.createTopology());
        while (true){
            //Will wait for a fail
            Thread.sleep(1000);
        }

    }

}
