package ocm.hdh.rabbitmq.queues;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * @author 华仔
 * @Description 单发送多接收
 *      1. 使用“task_queue”声明消息队列，并使消息队列durable
 *      2. 在使用channel.basicConsume接收消息时使autoAck为false，即不自动会发ack，由channel.basicAck()在消息处理完成后发送消息。
 *      3. 使用了channel.basicQos(1)保证在接收端一个消息没有处理完时不会接收另一个消息，
 *          即接收端发送了ack后才会接收下一个消息。在这种情况下发送端会尝试把消息发送给下一个not busy的接收端。
 * @date 2018年08月15日 14:42
 **/
public class Worker {


    public Worker (String queueName,final String consumerName){

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        //打开connection 和 channel
        Connection connection = null;

        try{

            connection = factory.newConnection();
            final Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);

            // 公平调度：
            // 告诉RabbitMQ，再同一时刻，不要发送超过1条消息给一个工作者（worker），直到它已经处理了上一条消息并且作出了响应。
            channel.basicQos(1);
            System.out.println(" [*] Waiting for messages.");

            Consumer consumer = new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                    String message = new String(body,"UTF-8");
                    System.out.println("[" + consumerName + "]" +"Received '" + message + "'");
                    try {
                        doWork(message);
                    }catch(Exception e){
                        e.printStackTrace();
                    } finally {
                        System.out.println("[" + consumerName + "]" + "basicAck  Done");
                        //手动确认
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    }

                }
            };

            channel.basicConsume(queueName, false, consumer);

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private static void doWork(String task) throws InterruptedException {

        // 读取到 '.' 该worker对应的线程停止1秒
        for (char ch: task.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }

    }


}
