package ocm.hdh.rabbitmq.single_demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 单发送单接收
 * @date 2018年08月15日 14:30
 **/
public class Recv {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection conn = connectionFactory.newConnection();
        Channel channel = conn.createChannel();

//	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

//	    channel.basicConsume(QUEUE_NAME, true, consumer);

        while (true) {
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(" Consumer have received '" + message + "'");
                }
            };
            channel.basicConsume(QUEUE_NAME, true, consumer);
        }

    }

}
