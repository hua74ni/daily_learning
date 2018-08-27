package ocm.hdh.rabbitmq.topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 主题
 * @date 2018年08月16日 16:54
 **/
public class ReceivedLogsTopic1 {

    private  static  final String EXCHANGE_NAME = "topic_logs" ;

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();

        String [] messageArr = new String [] {"#.rabbit.#"};

        if(messageArr.length < 1){
            System.exit(1);
        }

        for (String string:
                messageArr) {
            channel.queueBind(queueName, EXCHANGE_NAME, string);
        }

        System.out.println("[*] 等待消息。退出请按 CTRL +　C");

        Consumer consumer = new DefaultConsumer(channel){

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                String message = new String(body, "UTF-8");
                System.out.println("[X] Received '" + envelope.getRoutingKey() + "' : '" + message + "'");

            }

        };

        channel.basicConsume(queueName,true ,consumer);

    }

}
