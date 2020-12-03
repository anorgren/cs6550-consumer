package cs6550.assignment3.com;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cs6550.assignment3.com.Data.DataSource;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class Main {
    private static final String HOST = "amqps://b-7d4be5be-0faa-4d44-9138-95a16106e86b.mq.us-west-2.amazonaws.com:5671";

//    public static void main(String args[]) {
//        int poolSize = DataSource.getPOOL_SIZE();
//
//        try {
//
//            ConnectionFactory connectionFactory = new ConnectionFactory();
//            connectionFactory.setUri(HOST);
//            connectionFactory.setPort(5671);
//            connectionFactory.setAutomaticRecoveryEnabled(true);
//            connectionFactory.setUsername("admin");
//            connectionFactory.setPassword("password12345");
//
//            Connection connection = connectionFactory.newConnection();
//
//            IntStream.range(0, poolSize).forEach(val -> {
//                Thread dbWriteThread = new Thread(new DatabaseWriteThread(connection));
//                dbWriteThread.start();
//            });
//        } catch ( Exception e ) {
//            System.err.println("Error with queue:");
//            e.printStackTrace();
//        }
//    }

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
