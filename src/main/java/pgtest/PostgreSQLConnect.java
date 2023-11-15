package pgtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.Properties;
import java.sql.Timestamp;
import java.util.Date;
public class PostgreSQLConnect {
    private static final String DB_URL = "jdbc:postgresql://10.81.1.194:8432,10.81.1.194:11432,10.81.1.194:12432,10.81.1.194:9432,10.81.1.194:10432/postgres?targetServerType=preferPrimary&hostRecheckSeconds=10";
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres";

    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            Properties properties = new Properties();
            properties.setProperty("user", USER);
            properties.setProperty("password", PASSWORD);
            connection = DriverManager.getConnection(DB_URL, properties);
            while (true) {

                String sqlQuery = "INSERT INTO employees (first_name, last_name, email, hire_date) VALUES ('John', 'Doe', 'johndoe@example.com', '2023-10-26');";
                try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery))
                {
                    int rowsAffected = preparedStatement.executeUpdate();
                    Timestamp timestamp = new Timestamp(new Date().getTime());
                    System.out.println("Timestamp: " + timestamp + " Insert Done: Rows affected: " + rowsAffected);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (SQLException e) {
                   e.printStackTrace();
                }
                sqlQuery = "select * from employees;";
                    // Execute the query
                try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery))
                {
                    ResultSet rs = preparedStatement.executeQuery();
                    Timestamp timestamp = new Timestamp(new Date().getTime());
                    System.out.println("Timestamp: " + timestamp + " Select Done");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (SQLException e) {
                   e.printStackTrace();
                }
                Thread.sleep(1000);
                if (connection.isClosed()) {
                    connection = DriverManager.getConnection(DB_URL, properties);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
