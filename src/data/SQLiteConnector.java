package data;

import util.Config;
import util.ResourcesLoader;
import util.SimpleLog;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLiteConnector implements Connector{

    private Connection connection = null;

    private boolean connected = false;

    private static final String DB_PATH = "dummydhtdb";

    @Override
    public boolean isConnected() {
        if (connection != null) {
            try {
                connected = connected && !connection.isClosed();
            } catch (SQLException e) {
                e.printStackTrace();
                connected = false;
            }
        }

        return connected;
    }

    @Override
    public Connection reconnect() {
        if (connection != null) {
            try {
                connection.close();
                connected = false;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return getConnection();
    }

    @Override
    public Connection getConnection() {
        if (!connected) {
            try {
                DriverManager.registerDriver(new org.sqlite.JDBC());
                String dbDirPath = ResourcesLoader.getParentDirOfProgramPath() + File.separator + DB_PATH;
                File dir = new File(dbDirPath);
                if (!dir.exists()) {
                    dir.mkdir();
                }
                String dbFilePath = dbDirPath + File.separator + Config.getInstance().getAddress() + "." + Config.getInstance().getPort() + ".db";
                String url = "jdbc:sqlite:" + dbFilePath;
                SimpleLog.v(url);
                connection = DriverManager.getConnection(url);
                createTables();
                connected = true;
            } catch (Exception e) {
                connected = false;
                e.printStackTrace();
            }
        }
        return connection;
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                connection.close();
                connected = false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createHistoricalLoadInfoTable(String mode, String scheme) {
        String sql = "CREATE TABLE IF NOT EXISTS " + DummyDhtTables.HISTORICAL_LOAD_INFO.getName() +"(\n" +
                "            report_time INTEGER,\n" +
                "            node_id TEXT,\n" +
                "            file_load INTEGER,\n" +
                "            size_of_files INTEGER,\n" +
                "            read_load INTEGER,\n" +
                "            write_load INTEGER,\n" +
                "            number_of_miss INTEGER,\n" +
                "            number_of_lock_conflicts INTEGER,\n" +
                "            number_of_hits INTEGER,\n" +
                "            tag INTEGER\n" +
                ");";

        try{
            Statement stmt = connection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createLoadInfoTable(String mode, String scheme) {
        String sql = "CREATE TABLE IF NOT EXISTS " + DummyDhtTables.LOAD_INFO.getName() + "(\n" +
                "            report_time INTEGER,\n" +
                "            node_id TEXT,\n" +
                "            file_load INTEGER,\n" +
                "            size_of_files INTEGER,\n" +
                "            read_load INTEGER,\n" +
                "            write_load INTEGER,\n" +
                "            number_of_miss INTEGER,\n" +
                "            number_of_lock_conflicts INTEGER,\n" +
                "            number_of_hits INTEGER,\n" +
                "            tag INTEGER\n" +
                ");";

        try{
            Statement stmt = connection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createStatInfoTable (String mode, String scheme) {
        String sql = "CREATE TABLE IF NOT EXISTS " + DummyDhtTables.STAT_INFO.getName() + "(\n" +
                "            start_time INTEGER,\n" +
                "            end_time INTEGER,\n" +
                "            elapsed INTEGER,\n" +
                "            header TEXT,\n" +
                "            entry_token TEXT,\n" +
                "            type TEXT,\n" +
                "            size INTEGER,\n" +
                "            tag INTEGER\n" +
                ");";

        try{
            Statement stmt = connection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createTables() {
        String mode = Config.getInstance().getMode();
        String scheme = Config.getInstance().getScheme();
        createHistoricalLoadInfoTable(mode, scheme);
        createLoadInfoTable(mode, scheme);
        createStatInfoTable(mode, scheme);
    }
}
