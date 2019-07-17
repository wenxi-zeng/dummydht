package data;

import java.sql.Connection;

public interface Connector {
    boolean isConnected();
    Connection reconnect();
    Connection getConnection();
    void close();
}
