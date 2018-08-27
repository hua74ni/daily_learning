package com;

import org.apache.zookeeper.*;

/**
 * @Description //TODO
 * by 华仔 创建.
 **/
public class ZookeeperDemo {

    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) {
        try {
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("触发事件：" + event.getType());
                }
            });
            zooKeeper.create("/kafka", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(new String(zooKeeper.getData("/kafka", true, null)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
