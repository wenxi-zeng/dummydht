package data;

import loadmanagement.LoadInfo;
import statmanagement.StatInfo;
import util.Config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class DummyDhtRepository {
    private static final String TABLE_STAT_INFO = "statinfo";
    private static final String TABLE_LOAD_INFO = "loadinfo";
    private static final String TABLE_HISTORICAL_LOAD_INFO = "historicalloadinfo";

    private Connector connector;

    private Connection session;

    private ExecutorService executor;

    private Queue<PreparedStatement> queue;

    private static volatile DummyDhtRepository instance = null;

    private DummyDhtRepository() {
        connector = new Connector();
        connector.setServer(Config.getInstance().getDataServer());
        executor = Executors.newFixedThreadPool(2);
        queue = new LinkedList<PreparedStatement>(){
            @Override
            public boolean add(PreparedStatement info) {
                boolean result = super.add(info);
                process();

                return result;
            }
        };
    }

    public static DummyDhtRepository getInstance() {
        if (instance == null) {
            synchronized(DummyDhtRepository.class) {
                if (instance == null) {
                    instance = new DummyDhtRepository();
                }
            }
        }

        return instance;
    }

    public void process() {
        executor.execute(this::consume);
    }

    private void consume() {
        while (!queue.isEmpty()) {
            PreparedStatement statement = queue.poll();
            try {
                open();
                statement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        close();
    }

    public void insertLoadInfoBatch(List<LoadInfo> infoList, boolean isHistorical) {
        for (LoadInfo info : infoList) {
            insertLoadInfo(info, isHistorical);
        }
    }

    public void insertLoadInfo(LoadInfo info, boolean isHistorical) {
        if (isHistorical)
            insertHistoricalLoadInfo(info);
        else
            insertLoadInfo(info);
    }

    public void insertLoadInfo(LoadInfo info) {
        PreparedStatement statement = null;
        try {
            statement = session.prepareStatement(
                    "INSERT INTO " + TABLE_LOAD_INFO + " (report_time, node_id, file_load, number_of_hits, number_of_lock_conflicts, number_of_miss, read_load, size_of_files, write_load) " +
                            "VALUES (?, ?, ?, ? , ? , ?, ?, ?, ?)");
            statement.setTimestamp(1, new Timestamp(info.getReportTime()));
            statement.setString(2, info.getNodeId());
            statement.setLong(3, info.getFileLoad());
            statement.setLong(4, info.getNumberOfHits());
            statement.setLong(5, info.getNumberOfLockConflicts());
            statement.setLong(6, info.getNumberOfMiss());
            statement.setLong(7, info.getReadLoad());
            statement.setLong(8, info.getSizeOfFiles());
            statement.setLong(9, info.getWriteLoad());

            queue.add(statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertHistoricalLoadInfo(LoadInfo info) {
        PreparedStatement statement = null;
        try {
            statement = session.prepareStatement(
                    "INSERT INTO " + TABLE_HISTORICAL_LOAD_INFO + " (report_time, node_id, file_load, number_of_hits, number_of_lock_conflicts, number_of_miss, read_load, size_of_files, write_load) " +
                            "VALUES (?, ?, ?, ? , ? , ?, ?, ?, ?)");
            statement.setTimestamp(1, new Timestamp(info.getReportTime()));
            statement.setString(2, info.getNodeId());
            statement.setLong(3, info.getFileLoad());
            statement.setLong(4, info.getNumberOfHits());
            statement.setLong(5, info.getNumberOfLockConflicts());
            statement.setLong(6, info.getNumberOfMiss());
            statement.setLong(7, info.getReadLoad());
            statement.setLong(8, info.getSizeOfFiles());
            statement.setLong(9, info.getWriteLoad());

            queue.add(statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertStatInfo(StatInfo info) {
        PreparedStatement statement = null;
        try {
            statement = session.prepareStatement(
                    "INSERT INTO " + TABLE_STAT_INFO + " (entry_token, start_time, header, elapsed, end_time, type) " +
                            "VALUES (?, ?, ?, ? , ? , ?)");
            statement.setString(1, info.getToken());
            statement.setTimestamp(2, new Timestamp(info.getStartTime()));
            statement.setString(3, info.getHeader());
            statement.setLong(4, info.getElapsed());
            statement.setTimestamp(5, new Timestamp(info.getEndTime()));
            statement.setString(6, info.getType());

            queue.add(statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void open() {
        if (session == null || !connector.isConnected()) {
            session = connector.reconnect();
        }
    }

    public void close() {
        connector.close();
        session = null;
    }
}
