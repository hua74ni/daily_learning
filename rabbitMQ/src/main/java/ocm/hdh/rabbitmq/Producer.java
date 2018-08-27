package ocm.hdh.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 单发送单接收
 * @date 2018年08月14日 17:42
 **/
public class Producer {

    public static void main(String[] args) throws IOException, TimeoutException {

        // 链接是对socket链接的抽象，并且负责版本协议的协商和认证。
        // 这里我们链接了本地服务器的代理，也就是localhost。
        // 如果我们想链接一个不同机器的代理，我们仅需要修改这里的名称或者ip地址即可。
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("localhost");
        // 创建 rabbitMQ工厂连接
        Connection conn = factory.newConnection();

        // 获取 信道
        Channel channel = conn.createChannel();

        // 声明交换器
        String exchangeName = "hello-exchange";
        channel.exchangeDeclare(exchangeName, "direct", true);


        String routingKey = "hola";

        // 发布消息
        byte [] messageBodyBytes = "quit".getBytes();

        channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

        channel.close();
        conn.close();

    }

}
