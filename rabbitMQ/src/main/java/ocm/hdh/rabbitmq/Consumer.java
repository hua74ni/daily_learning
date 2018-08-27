package ocm.hdh.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 单发送单接收
 * @date 2018年08月14日 17:42
 **/
public class Consumer {

    public static void main(String[] args) throws IOException, TimeoutException {

        // 创建 RabbitMQ工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setHost("localhost");

        // 建立 RabbitMQ工厂的连接
        Connection conn = connectionFactory.newConnection();

        // 创建 RabbitMQ工厂的连接的信道
        final Channel channel = conn.createChannel();

        // 声明交换器
        String exchangeName = "hello-exchange";
        channel.exchangeDeclare(exchangeName, "direct", true);

        // 声明队列
        String queueName = channel.queueDeclare().getQueue();
        String routingKey = "hola";

        // 绑定队列，通过路由键 hola 将队列和交换器绑定起来
        channel.queueBind(queueName, exchangeName, routingKey);

        while (true){

            // 消费信息
            boolean autoAck =false;
            String consumerTag = "";
            channel.basicConsume(queueName, autoAck, consumerTag, new DefaultConsumer(channel){

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {


                    String routingKey = envelope.getRoutingKey();
                    String contentType = properties.getContentType();
                    System.out.println("消费的路由键：" + routingKey);
                    System.out.println("消费的内容类型：" + contentType);

                    long deliveryTag = envelope.getDeliveryTag();

                    // 确认消息
                    channel.basicAck(deliveryTag,false);
                    System.out.println("消费的消息体内容：");
                    String bodyStr = new String(body, "UTF-8");
                    System.out.println(bodyStr);

                }
            });

        }

    }

}
