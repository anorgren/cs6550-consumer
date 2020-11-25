package cs6550.assignment3.com;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cs6550.assignment3.com.Data.DataSource;

import java.util.stream.IntStream;

public class Main {
    private static final String HOST = "amqps://b-e8c5ee0c-9e15-4aa0-b491-9fcbe7800bb5.mq.us-west-2.amazonaws.com:5671";

    public static void main(String args[]) {
        int poolSize = DataSource.getPOOL_SIZE();

        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUri(HOST);
            connectionFactory.setPort(5671);
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
