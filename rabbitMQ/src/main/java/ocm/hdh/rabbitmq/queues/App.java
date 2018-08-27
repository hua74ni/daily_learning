package ocm.hdh.rabbitmq.queues;

/**
 * @author 华仔
 * @Description 单发送多接收
 * @date 2018年08月15日 16:49
 **/
public class App {

    private static final String TASK_QUEUE_NAME = "task_queue";


    /**
     * 多个消费者同时消费一个队列
     * 1. 使用“task_queue”声明消息队列，并使消息队列durable
     * 2. 在使用channel.basicConsume接收消息时使autoAck为false，即不自动会发ack，由channel.basicAck()在消息处理完成后发送消息。
     * 3. 使用了channel.basicQos(1)保证在接收端一个消息没有处理完时不会接收另一个消息，
     *    即接收端发送了ack后才会接收下一个消息。在这种情况下发送端会尝试把消息发送给下一个not busy的接收端。
     */

    public static void main(String[] args) {

        Worker worker1 = new Worker(TASK_QUEUE_NAME, "consumer1");
        Worker worker2 = new Worker(TASK_QUEUE_NAME, "consumer2");
        Worker worker3 = new Worker(TASK_QUEUE_NAME, "consumer3");
        Worker worker4 = new Worker(TASK_QUEUE_NAME, "consumer4");

    }

}
