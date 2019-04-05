package loadmanagement;

import commonmodels.GlobalLoadListener;
import commonmodels.PhysicalNode;
import data.DummyDhtRepository;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalLoadInfoManager {

    private Map<String, LoadInfo> globalLoadInfo;

    private List<LoadInfo> historicalLoadInfo;

    private final DummyDhtRepository repo;

    private List<GlobalLoadListener> callBacks;

    private static volatile GlobalLoadInfoManager instance = null;

    private GlobalLoadInfoManager() {
        globalLoadInfo = new ConcurrentHashMap<>();
        historicalLoadInfo = new ArrayList<>();
        repo = DummyDhtRepository.getInstance();
        callBacks = new ArrayList<>();
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

    public void subscribe(GlobalLoadListener callBack) {
        callBacks.add(callBack);
    }

    public void unsubscribe(GlobalLoadListener callBack) {
        callBacks.remove(callBack);
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
        repo.put(info);
        print();
        announce();
    }

    public void update(List<PhysicalNode> nodes) {
        List<String> nodeIdList = new ArrayList<>(globalLoadInfo.keySet());
        for (PhysicalNode node : nodes) {
            nodeIdList.remove(node.getFullAddress());
        }
        consolidate(nodeIdList);
        print();
    }

    private void consolidate(List<String> nodeIdList) {
        for (String node  : nodeIdList) {
            consolidate(node);
        }
    }

    private void consolidate(String nodeId) {
        LoadInfo info = globalLoadInfo.remove(nodeId);
        if (info != null) {
            info.setReportTime(System.currentTimeMillis());
            info.setConsolidated(true);
            historicalLoadInfo.add(info);
            repo.put(info);
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

        SimpleLog.v(builder.toString());
    }

    private void announce() {
        for (GlobalLoadListener callBack : callBacks) {
            callBack.onLoadUpdated(new ArrayList<>(globalLoadInfo.values()));
        }
    }
}
