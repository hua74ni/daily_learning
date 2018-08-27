package ocm.hdh.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description 主题
 *              *（星号）可以替代一个单词。
 *              ＃（hash）可以替换零个或多个单词。
 *
 * note： Topic exchange
 *        Topic exchange is powerful and can behave like other exchanges.
 *
 *        When a queue is bound with "#" (hash) binding key - it will receive all the messages, regardless of the routing key - like in fanout exchange.
 *
 *        When special characters "*" (star) and "#" (hash) aren't used in bindings, the topic exchange will behave just like a direct one.
 *
 * @date 2018年08月16日 16:39
 **/
public class EmitLogTopic {

    private  static  final String EXCHANGE_NAME = "topic_logs" ;

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        String [] severityArr = new String [] {"lazy.dog"};
        String [] messageArr = new String [] {"懒惰的狗狗"};

        String severity = getSeverity(severityArr);
        String message = getMessage(messageArr);

        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
        System.out.println("[x] Sent'" + severity + "'：'" + message + "'");

        channel.close();
        connection.close();

    }

    private static String getSeverity(String[] strings){
        if (strings.length < 1) {
            return "info";
        }
        return strings[0];
    }

    private static String getMessage(String[] strings){
        if (strings.length < 2) {
            return "Hello World!";
        }
        return joinStrings(strings, " ", 1);
    }

    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0 ) {
            return "";
        }
        if (length < startIndex ) {
            return "";
        }
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }

}
