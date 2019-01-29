package data;

import com.datastax.driver.core.Session;
import loadmanagement.LoadInfo;
import statmanagement.StatInfo;
import util.Config;

public class CassandraHelper {

    private CassandraConnector connector;

    private DummyDhtRepository repository;

    private String node;

    private int port;

    private static volatile CassandraHelper instance = null;

    private CassandraHelper() {
        connector = new CassandraConnector();
        String[] server = Config.getInstance().getDataServer().split(":");
        node = server[0];
        port = Integer.valueOf(server[1]);
    }

    public static CassandraHelper getInstance() {
        if (instance == null) {
            synchronized(CassandraHelper.class) {
                if (instance == null) {
                    instance = new CassandraHelper();
                }
            }
        }

        return instance;
    }

    public void open() {
        connector.connect(node, port);
        Session session = connector.getSession();

        SchemaRepository sr = new SchemaRepository(session);
        sr.useKeyspace("coogle");

        repository = new DummyDhtRepository(session);
    }

    public void insertLoadInfo(LoadInfo loadInfo, boolean isHistorical) throws Exception {
        if (repository == null) throw new Exception("DummyDhtRepository Exception: not opened");

        if (isHistorical) {
            repository.insertHistoricalLoadInfo(loadInfo);
        }
        else {
            repository.insertLoadInfo(loadInfo);
        }
    }

    public void insertStatInfo(StatInfo statInfo) throws Exception {
        if (repository == null) throw new Exception("DummyDhtRepository Exception: not opened");
        repository.insertStatInfo(statInfo);
    }

    public void close() {
        connector.close();
        repository = null;
    }
}
