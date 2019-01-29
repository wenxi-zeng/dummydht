package loadmanagement;

import commonmodels.PhysicalNode;
import data.CassandraHelper;
import util.SimpleLog;

import java.util.*;

public class GlobalLoadInfoManager {

    private Map<String, LoadInfo> globalLoadInfo;

    private List<LoadInfo> historicalLoadInfo;

    private static volatile GlobalLoadInfoManager instance = null;

    private GlobalLoadInfoManager() {
        globalLoadInfo = new HashMap<>();
        historicalLoadInfo = new ArrayList<>();
    }

    public static GlobalLoadInfoManager getInstance() {
        if (instance == null) {
            synchronized(GlobalLoadInfoManager.class) {
                if (instance == null) {
                    instance = new GlobalLoadInfoManager();
                }
            }
        }

        return instance;
    }

    public List<LoadInfo> getGlobalLoadInfo() {
        return new ArrayList<>(globalLoadInfo.values());
    }

    public List<LoadInfo> getHistoricalLoadInfo() {
        return historicalLoadInfo;
    }

    public void update(LoadInfo info) {
        info.setReportTime(System.currentTimeMillis());
        globalLoadInfo.put(info.getNodeId(), info);
        updateToDatabase(info, false);
        print();
    }

    public void update(List<PhysicalNode> nodes) {
        Set<String> nodeIdList = globalLoadInfo.keySet();
        for (PhysicalNode node : nodes) {
            nodeIdList.remove(node.getFullAddress());
        }
        consolidate(nodeIdList);
        print();
    }

    private void consolidate(Set<String> nodeIdList) {
        for (String node  : nodeIdList) {
            consolidate(node);
        }
    }

    private void consolidate(String nodeId) {
        LoadInfo info = globalLoadInfo.remove(nodeId);
        if (info != null) {
            info.setReportTime(System.currentTimeMillis());
            historicalLoadInfo.add(info);
            updateToDatabase(info, true);
        }
    }

    public void print() {
        StringBuilder builder = new StringBuilder();
        builder.append("Global load info\n");
        for (LoadInfo info : globalLoadInfo.values()) {
            builder.append(info.toString()).append('\n');
        }
        builder.append("\nHistorical load info\n");
        for (LoadInfo info : historicalLoadInfo) {
            builder.append(info.toString()).append('\n');
        }

        SimpleLog.r(builder.toString());
    }

    private void updateToDatabase(LoadInfo info, boolean isHistorical) {
        CassandraHelper db = CassandraHelper.getInstance();
        db.open();
        try {
            db.insertLoadInfo(info, isHistorical);
        } catch (Exception e) {
            e.printStackTrace();
        }
        db.close();
    }
}
