package ocm.hdh.rabbitmq.single_demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 单发送单接收
 * @date 2018年08月15日 14:29
 **/
public class Send {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection conn = connectionFactory.newConnection();
        Channel channel = conn.createChannel();

        /**
         * queueDeclare
         * 第一个参数表示队列名称
         * 第二个参数为是否持久化（true表示是，队列将在服务器重启时生存）
         * 第三个参数为是否是独占队列（创建者可以使用的私有队列，断开后自动删除）
         * 第四个参数为当所有消费者客户端长时间连接断开时是否自动删除队列
         * 第五个参数为队列的其他参数
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello World";

        /**
         * basicPublish
         * 第一个参数为交换机名称
         * 第二个参数为队列映射的路由key
         * 第三个参数为消息的其他属性
         * 第四个参数为发送信息的主体
         */
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        conn.close();

    }

}
