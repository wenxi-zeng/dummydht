package data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connector {
    private String status = "not connected";
    private String server = "localhost";
    private String schema = "dummydht";
    private String user = "alien";
    private String password = "alien1";

    private Connection connection = null;

    private boolean connected = false;

    public void setAuthentication(String username, String pass) {
        user = username;
        password = pass;
    }

    public void setSchema(String schemaname) {
        schema = schemaname;
    }

    public String getSchema() {
        return schema;
    }

    public void setServer(String servername) {
        server = servername;
    }

    public String getConnetionStatus() {
        return status;
    }

    public boolean isConnected() {
        if (connection != null) {
            try {
                connected = connected && !connection.isClosed();
            } catch (SQLException e) {
                e.printStackTrace();
                connected = false;
            }
        }
        if (connected)
            status = "connected";
        else {
            status = "not connected";
        }
        return connected;
    }

    public Connection reconnect() {
        if (connection != null) {
            try {
                connection.close();
                connected = false;
            } catch (SQLException e) {
                status = e.getMessage();
                e.printStackTrace();
            }
        }
        return getConnection();
    }

    public Connection getConnection() {
        if (!connected) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                String url = "jdbc:mysql://" + server + "/" + schema + "?user="
                        + user + "&password=" + password;
                connection = (Connection) DriverManager.getConnection(url);
                status = "connected";
                connected = true;
            } catch (Exception e) {
                status = e.getMessage();
            }
        }
        return connection;
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        status = "not connected";
    }
}