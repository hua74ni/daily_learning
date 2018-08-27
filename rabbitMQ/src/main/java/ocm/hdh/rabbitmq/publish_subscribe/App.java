package ocm.hdh.rabbitmq.publish_subscribe;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author 华仔
 * @Description //TODO
 * @date 2018年08月15日 16:49
 **/
public class App {

    public static void main(String[] args) throws IOException, TimeoutException {

        ReceiveLogs receiveLogs1 = new ReceiveLogs("consumer1");
        ReceiveLogs receiveLogs2 = new ReceiveLogs("consumer2");

    }

}
