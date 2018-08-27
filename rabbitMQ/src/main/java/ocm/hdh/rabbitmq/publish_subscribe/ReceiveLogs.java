package ocm.hdh.rabbitmq.publish_subscribe;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description //TODO
 * @date 2018年08月15日 16:15
 **/
public class ReceiveLogs {

    private static final String EXCHANGE_NAME = "logs";

    public ReceiveLogs (final String consumerName) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        // 不需要指定routing key，设置了fanout,指了也没有用.
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" ["+ consumerName +"] Waiting for messages.");

        final boolean autoAck = false ;

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                String routingKey = envelope.getRoutingKey();
                String contentType = properties.getContentType();
                long deliveryTag = envelope.getDeliveryTag();
                System.out.println("消费的路由键：" + routingKey);
                System.out.println("消费的内容类型：" + contentType);

                // 确认消息
                channel.basicAck(deliveryTag,autoAck);
                System.out.println(" ["+ consumerName +"]" + "消费的消息体内容：");
                String bodyStr = new String(body, "UTF-8");
                System.out.println(bodyStr);

            }
        };

        // 关闭自动确认消息
        channel.basicConsume(queueName, false, consumer);

    }

}
