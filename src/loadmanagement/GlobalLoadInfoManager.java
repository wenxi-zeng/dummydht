package loadmanagement;

import commonmodels.PhysicalNode;
import data.DummyDhtRepository;
import util.SimpleLog;

import java.util.*;

public class GlobalLoadInfoManager {

    private Map<String, LoadInfo> globalLoadInfo;

    private List<LoadInfo> historicalLoadInfo;

    private final DummyDhtRepository repo;

    private static volatile GlobalLoadInfoManager instance = null;

    private GlobalLoadInfoManager() {
        globalLoadInfo = new HashMap<>();
        historicalLoadInfo = new ArrayList<>();
        repo = DummyDhtRepository.getInstance();
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
        repo.insertLoadInfo(info, false);
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
            repo.insertLoadInfo(info, true);
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
}
