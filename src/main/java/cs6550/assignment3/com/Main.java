package cs6550.assignment3.com;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cs6550.assignment3.com.Data.DataSource;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class Main {
    private static final String HOST = "amqps://b-6209b746-4cba-488c-9b28-106ac3f13ba6.mq.us-west-2.amazonaws.com:5671";

    public static void main(String args[]) {
        int poolSize = DataSource.getPOOL_SIZE();

        try {

            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUri(HOST);
            connectionFactory.setPort(5671);
            connectionFactory.setAutomaticRecoveryEnabled(true);
            connectionFactory.setUsername("admin");
            connectionFactory.setPassword("password12345");

            IntStream.range(0, poolSize).forEach(val -> {
                try {
                    Connection connection = connectionFactory.newConnection();
                    Thread dbWriteThread = new Thread(new DatabaseWriteThread(connection));
                    dbWriteThread.start();
                } catch (Exception e) {
                    System.err.println("Error with connection:");
                    e.printStackTrace();
                }
            });
        } catch ( Exception e ) {
            System.err.println("Error with queue:");
            e.printStackTrace();
        }
    }
}
