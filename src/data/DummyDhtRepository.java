package data;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import loadmanagement.LoadInfo;
import statmanagement.StatInfo;

import java.util.Date;
import java.util.List;


public class DummyDhtRepository {
    private static final String TABLE_STAT_INFO = "statinfo";
    private static final String TABLE_LOAD_INFO = "loadinfo";
    private static final String TABLE_HISTORICAL_LOAD_INFO = "historicalloadinfo";

    private Session session;

    public DummyDhtRepository(Session session) {
        this.session = session;
    }

    public void insertLoadInfoBatch(List<LoadInfo> infoList, boolean isHistorical) {
        for (LoadInfo info : infoList) {
            if (isHistorical)
                insertHistoricalLoadInfo(info);
            else
                insertLoadInfo(info);
        }
    }

    public void insertLoadInfo(LoadInfo info) {
        PreparedStatement statement = session.prepare(
                "INSERT INTO " + TABLE_LOAD_INFO + " (report_time, node_id, file_load, number_of_hits, number_of_lock_conflicts, number_of_miss, read_load, size_of_files, write_load) " +
                        "VALUES (?, ?, ?, ? , ? , ?, ?, ?, ?) IF NOT EXISTS;");

        BoundStatement boundStatement = statement.bind(
                new Date(info.getReportTime()),
                info.getNodeId(),
                info.getFileLoad(),
                info.getNumberOfHits(),
                info.getNumberOfLockConflicts(),
                info.getNumberOfMiss(),
                info.getReadLoad(),
                info.getSizeOfFiles(),
                info.getWriteLoad()
        );

        session.execute(boundStatement);
    }

    public void insertHistoricalLoadInfo(LoadInfo info) {
        PreparedStatement statement = session.prepare(
                "INSERT INTO " + TABLE_HISTORICAL_LOAD_INFO + " (report_time, node_id, file_load, number_of_hits, number_of_lock_conflicts, number_of_miss, read_load, size_of_files, write_load) " +
                        "VALUES (?, ?, ?, ? , ? , ?, ?, ?, ?) IF NOT EXISTS;");

        BoundStatement boundStatement = statement.bind(
                new Date(info.getReportTime()),
                info.getNodeId(),
                info.getFileLoad(),
                info.getNumberOfHits(),
                info.getNumberOfLockConflicts(),
                info.getNumberOfMiss(),
                info.getReadLoad(),
                info.getSizeOfFiles(),
                info.getWriteLoad()
        );

        session.execute(boundStatement);
    }

    public void insertStatInfo(StatInfo info) {
        PreparedStatement statement = session.prepare(
                "INSERT INTO " + TABLE_STAT_INFO + " (entry_token, start_time, header, elapsed, end_time, type) " +
                        "VALUES (?, ?, ?, ? , ? , ?) IF NOT EXISTS;");

        BoundStatement boundStatement = statement.bind(
                info.getToken(),
                new Date(info.getStartTime()),
                info.getHeader(),
                info.getElapsed(),
                new Date(info.getEndTime()),
                info.getType()
        );

        session.execute(boundStatement);
    }
}
