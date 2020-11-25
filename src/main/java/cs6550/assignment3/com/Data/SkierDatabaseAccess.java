package cs6550.assignment3.com.Data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SkierDatabaseAccess {

    public static void insertLiftRide(
            String resortID,
            String liftID,
            String skierID,
            String dayID,
            String time,
            int vertical) throws SQLException {

        String insertSQLStatement =
                "INSERT INTO liftRides (resortID, liftID, skierID, dayID, runTime, vertical)"
                        + "VALUES(?,?,?,?,?,?)";
        try (Connection connection = DataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQLStatement)) {

            preparedStatement.setString(1, resortID);
            preparedStatement.setString(2, liftID);
            preparedStatement.setString(3, skierID);
            preparedStatement.setString(4, dayID);
            preparedStatement.setString(5, time);
            preparedStatement.setInt(6, vertical);

            preparedStatement.executeUpdate();
        }
    }
}
