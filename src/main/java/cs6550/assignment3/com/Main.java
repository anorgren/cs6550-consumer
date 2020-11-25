package cs6550.assignment3.com;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cs6550.assignment3.com.Data.DataSource;

import java.util.stream.IntStream;

public class Main {
    private static final String HOST = "amqps://b-54863bfa-426a-425f-991b-9c83bd8fb830.mq.us-west-2.amazonaws.com:5671";

    public static void main(String args[]) {
        int poolSize = DataSource.getPOOL_SIZE();

        try {

            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUri(HOST);
            connectionFactory.setPort(5671);
            connectionFactory.setAutomaticRecoveryEnabled(true);
            connectionFactory.setUsername("admin");
            connectionFactory.setPassword("password12345");

            Connection connection = connectionFactory.newConnection();

            IntStream.range(0, poolSize).forEach(val -> {
                Thread dbWriteThread = new Thread(new DatabaseWriteThread(connection));
                dbWriteThread.start();
            });
        } catch ( Exception e ) {
            System.err.println("Error with queue:");
            e.printStackTrace();
        }
    }
}
