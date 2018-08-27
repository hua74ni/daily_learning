package ocm.hdh.rabbitmq.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 单发送多接收
 * @date 2018年08月15日 14:42
 **/
public class Task {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory=new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection=factory.newConnection();
        Channel channel=connection.createChannel();

        /**
         * 声明了另一个Queue，因为RabbitMQ不容许声明2个相同名称、配置不同的Queue
         * 持久化、非独占队列、非自动删除
         * 使用MessageProperties.PERSISTENT_TEXT_PLAIN使消息durable
         *
         * 官方：
         * When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to.
         * Two things are required to make sure that messages aren't lost: we need to mark both the queue and messages as durable.
         */
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        // 分发信息
        for (int i = 0; i < 10; i++) {
            String message = "Hello RabbitMQ"+ i + ".";
            channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println("NewTask send '"+message+"'");
        }

        connection.close();
        channel.close();

    }

}
