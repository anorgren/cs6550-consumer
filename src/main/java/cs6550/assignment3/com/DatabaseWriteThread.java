package cs6550.assignment3.com;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import cs6550.assignment3.com.Data.SkierDatabaseAccess;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

@AllArgsConstructor
public class DatabaseWriteThread implements Runnable {
    private static final String QUEUE_NAME_PERSISTENT = "SKIER_WRITE_QUEUE_PERSISTENT";
    private static final String QUEUE_NAME_NOT_PERSISTENT = "SKIER_WRITE_QUEUE_NOT_PERSISTENT";
    private final boolean AUTO_ACK = true;
    private final boolean IS_DURABLE = false;

    private Connection queueConnection;

    @Override
    public void run() {

        try {
            Channel channel = queueConnection.createChannel();
            channel.basicQos(1);
            channel.queueDeclare(QUEUE_NAME_NOT_PERSISTENT, IS_DURABLE, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
              String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
              String[] msgParts = msg.split(",");

              try {
                  String resortID = msgParts[0];
                  String dayID = msgParts[1];
                  String skierID = msgParts[2];
                  String time = msgParts[3];
                  String liftID = msgParts[4];
                  int vertical = Integer.parseInt(msgParts[5]);

                  System.out.println("Writing to db");
                  SkierDatabaseAccess.insertLiftRide(resortID, liftID, skierID, dayID, time, vertical);

              } catch (SQLException sqlE) {
                  System.err.println("Unable to write to database");
                  sqlE.printStackTrace();
              }
            };

            System.out.println("Waiting for message...");
            channel.basicConsume(QUEUE_NAME_NOT_PERSISTENT, AUTO_ACK, deliverCallback, consumerTag -> {});

        } catch (IOException e) {
            System.err.println("Unable to establish connection to queue");
            e.printStackTrace();
        }
    }
}
