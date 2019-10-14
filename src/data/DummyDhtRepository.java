package data;

import commonmodels.Queueable;
import filemanagement.BucketMigrateInfo;
import loadmanagement.LoadInfo;
import statmanagement.StatInfo;
import util.Config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class DummyDhtRepository {
    private final String TABLE_STAT_INFO;
    private final String TABLE_LOAD_INFO;
    private final String TABLE_HISTORICAL_LOAD_INFO;
    private final String TABLE_MIGRATE_INFO;

    private Connector connector;

    private Connection session;

    private ExecutorService executor;

    private BlockingQueue<Queueable> queue;

    private AtomicBoolean running;

    private final int tag;

    private static volatile DummyDhtRepository instance = null;

    private DummyDhtRepository() {
        this.connector = new SQLiteConnector();
        executor = Executors.newFixedThreadPool(2);
        queue = new LinkedBlockingQueue<>();
        running = new AtomicBoolean(true);
        String mode = Config.getInstance().getMode();
        String scheme = Config.getInstance().getScheme();
        tag = Config.getInstance().getTrialTag();
        TABLE_STAT_INFO = DummyDhtTables.STAT_INFO.getName();
        TABLE_LOAD_INFO = DummyDhtTables.LOAD_INFO.getName();
        TABLE_HISTORICAL_LOAD_INFO = DummyDhtTables.HISTORICAL_LOAD_INFO.getName();
        TABLE_MIGRATE_INFO = DummyDhtTables.MIGRATE_INFO.getName();

        registerShutdownHook();
        executor.execute(this::consume);
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


    public static void deleteInstance() {
        instance = null;
    }

    public void start() {
        executor.execute(this::consume);
    }

    public void stop() {
        executor.shutdownNow();
        deleteInstance();
    }

    public void put(Queueable queueable) {
        executor.execute(() -> produce(queueable));
    }

    private void produce(Queueable queueable) {
        try {
            queue.put(queueable);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void consume() {
        try {
            while (running.get()) {
                open();
                Queueable queueable = queue.take();
                if (queueable instanceof StatInfo)
                    insertStatInfo((StatInfo) queueable);
                else if (queueable instanceof LoadInfo)
                    insertLoadInfo((LoadInfo) queueable);
                else if (queueable instanceof BucketMigrateInfo)
                    insertMigrateInfo((BucketMigrateInfo) queueable);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void insertLoadInfo(LoadInfo info) {
        try {
            String table = info.isConsolidated() ? TABLE_HISTORICAL_LOAD_INFO : TABLE_LOAD_INFO;
            PreparedStatement statement = session.prepareStatement(
                    "INSERT INTO " + table + " (report_time, node_id, file_load, number_of_hits, number_of_lock_conflicts, number_of_miss, read_load, size_of_files, write_load, tag, serial_number) " +
                            "VALUES (?, ?, ?, ? , ? , ?, ?, ?, ?, ?, ?)");
            statement.setTimestamp(1, new Timestamp(info.getReportTime()));
            statement.setString(2, info.getNodeId());
            statement.setLong(3, info.getFileLoad());
            statement.setLong(4, info.getNumberOfHits());
            statement.setLong(5, info.getNumberOfLockConflicts());
            statement.setLong(6, info.getNumberOfMiss());
            statement.setLong(7, info.getReadLoad());
            statement.setLong(8, info.getSizeOfFiles());
            statement.setLong(9, info.getWriteLoad());
            statement.setInt(10, tag);
            statement.setLong(11, info.getSerialNumber());

            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertStatInfo(StatInfo info) {
        try {
            PreparedStatement statement = session.prepareStatement(
                    "INSERT INTO " + TABLE_STAT_INFO + " (entry_token, start_time, header, elapsed, end_time, type, size, tag) " +
                            "VALUES (?, ?, ?, ? , ? , ?, ?, ?)");
            statement.setString(1, info.getToken());
            statement.setTimestamp(2, new Timestamp(info.getStartTime()));
            statement.setString(3, info.getHeader());
            statement.setLong(4, info.getElapsed());
            statement.setTimestamp(5, new Timestamp(info.getEndTime()));
            statement.setString(6, info.getType());
            statement.setLong(7, info.getSize());
            statement.setInt(8, tag);

            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertMigrateInfo(BucketMigrateInfo info) {
        try {
            PreparedStatement statement = session.prepareStatement(
                    "INSERT INTO " + TABLE_MIGRATE_INFO + " (node_id, original_load, gentiles_load, gentile_load_map, caused_by_gentile, report_time, entry_token, tag) " +
                            "VALUES (?, ?, ?, ? , ? , ?, ?, ?)");
            statement.setString(1, info.getNodeId());
            statement.setLong(2, info.getOriginalBucketLoad());
            statement.setLong(3, info.getGentileBucketLoad());
            statement.setString(4, info.getGentileBucketMapString());
            statement.setString(5, String.valueOf(info.isCausedByGentile()));
            statement.setTimestamp(6, new Timestamp(info.getTimestamp()));
            statement.setString(7, info.getToken());
            statement.setInt(8, tag);

            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    public void open() {
        if (session == null || !connector.isConnected()) {
            session = connector.reconnect();
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    public void close() {
        connector.close();
        session = null;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }
}
