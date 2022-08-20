package hackerrank;

import java.sql.*;

public class JDBCUtils {


    private static String jdbcURL = "jdbc:hsqldb:file:testdb";
    private static String jdbcUsername = "SA";
    private static String jdbcPassword = "";

    static String  create = "CREATE TABLE event_table ( event_id VARCHAR(256), alert VARCHAR(256) , type VARCHAR(256), host VARCHAR(256), duration INTEGER)";
    static String insert = "INSERT INTO event_table(event_id,alert,type,host,duration) VALUES (?, ?, ?, ?, ?);";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public  static void createTable()  {

        try (Connection connection = JDBCUtils.getConnection();
             Statement st = connection.createStatement()){

            int i = st.executeUpdate(create);    // run the query

            if (i == -1) {
                System.out.println("db error : " + create);
            }

        } catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void insertRecord(String id,String alert,String type, String host, Integer duration) throws SQLException {


        try (Connection connection = JDBCUtils.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insert)) {
            preparedStatement.setString(1, id);
            preparedStatement.setString(2, alert);
            preparedStatement.setString(3, type);
            preparedStatement.setString(4, host);
            preparedStatement.setInt(5, duration);

            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Step 4: try-with-resource statement will auto close the connection.
    }
}
