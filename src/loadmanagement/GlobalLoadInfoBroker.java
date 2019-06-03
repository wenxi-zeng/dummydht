package loadmanagement;

import commonmodels.LoadChangeListener;
import commonmodels.PhysicalNode;
import data.DummyDhtRepository;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalLoadInfoBroker {

    private Map<String, LoadInfo> globalLoadInfo;

    private List<LoadInfo> historicalLoadInfo;

    private LoadInfo dummyInfo;

    private final DummyDhtRepository repo;

    private List<LoadChangeListener> callBacks;

    private static volatile GlobalLoadInfoBroker instance = null;

    private GlobalLoadInfoBroker() {
        globalLoadInfo = new ConcurrentHashMap<>();
        historicalLoadInfo = new ArrayList<>();
        repo = DummyDhtRepository.getInstance();
        callBacks = new ArrayList<>();
        dummyInfo = new LoadInfo().withNodeId(null);
    }

    public static GlobalLoadInfoBroker getInstance() {
        if (instance == null) {
            synchronized(GlobalLoadInfoBroker.class) {
                if (instance == null) {
                    instance = new GlobalLoadInfoBroker();
                }
            }
        }

        return instance;
    }

    public void subscribe(LoadChangeListener callBack) {
        callBacks.add(callBack);
    }

    public void unsubscribe(LoadChangeListener callBack) {
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
        // repo.put(info); moved to LoadInfoReporter
        announce();
    }

    public void update(List<PhysicalNode> nodes) {
        List<String> obsolete = new ArrayList<>(globalLoadInfo.keySet());
        List<String> newNodes = new ArrayList<>();
        for (PhysicalNode node : nodes) {
            obsolete.remove(node.getFullAddress());
            if (!globalLoadInfo.containsKey(node.getFullAddress()))
                newNodes.add(node.getFullAddress());
        }
        consolidate(obsolete);
        initialize(newNodes);
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

    private void initialize(List<String> nodeIdList) {
        for (String node  : nodeIdList) {
            globalLoadInfo.put(node, dummyInfo);
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

    private synchronized void announce() {
        for (LoadInfo info : globalLoadInfo.values()) {
            if (info.getNodeId() == null) return;
        }
        print();
        List<LoadInfo> infoList = new ArrayList<>(globalLoadInfo.values());
        for (String key : globalLoadInfo.keySet()) {
            globalLoadInfo.put(key, dummyInfo);
        }

        for (LoadChangeListener callBack : callBacks) {
            callBack.onLoadUpdated(infoList);
        }
    }
}
