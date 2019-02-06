package loadmanagement;

import commonmodels.GlobalLoadListener;
import commonmodels.PhysicalNode;
import data.DummyDhtRepository;
import util.Config;
import util.SimpleLog;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalLoadInfoManager {

    private Map<String, LoadInfo> globalLoadInfo;

    private List<LoadInfo> historicalLoadInfo;

    private final DummyDhtRepository repo;

    private final long lbUpperBound;

    private final long lbLowerBound;

    private List<GlobalLoadListener> callBacks;

    private static volatile GlobalLoadInfoManager instance = null;

    private GlobalLoadInfoManager() {
        globalLoadInfo = new ConcurrentHashMap<>();
        historicalLoadInfo = new ArrayList<>();
        repo = DummyDhtRepository.getInstance();
        lbUpperBound = Config.getInstance().getLoadBalancingUpperBound();
        lbLowerBound = Config.getInstance().getLoadBalancingLowerBound();
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
        observe();
    }

    public void update(List<PhysicalNode> nodes) {
        List<String> nodeIdList = new ArrayList<>(globalLoadInfo.keySet());
        for (PhysicalNode node : nodes) {
            nodeIdList.remove(node.getFullAddress());
        }
        consolidate(nodeIdList);
        print();
        observe();
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

        SimpleLog.r(builder.toString());
    }

    private void observe() {
        List<LoadInfo> lightNodes = new ArrayList<>();
        for (LoadInfo info : globalLoadInfo.values()) {
            if (info.getSizeOfFiles() < lbLowerBound) {
                lightNodes.add(info);
            }
        }

        if (lightNodes.size() < 1) {
            SimpleLog.v("All nodes are higher than load balancing lower bound, no need to balance");
        }
        else {
            boolean needTwoNodes = Config.getInstance().getScheme().equals(Config.SCHEME_ELASTIC);
            for (LoadInfo info : globalLoadInfo.values()) {
                if (info.getSizeOfFiles() > lbUpperBound) {
                    SimpleLog.v("Node " + info.getNodeId() + " is overloaded. Decreasing its load");

                    if (needTwoNodes)
                        onOverLoad(info, lightNodes.get(0));
                    else
                        onOverLoad(info);
                }
            }
        }
    }

    private void onOverLoad(LoadInfo loadInfo) {
        if (callBacks != null)
            for (GlobalLoadListener callBack : callBacks) {
                callBack.onOverload(loadInfo);
            }
    }

    private void onOverLoad(LoadInfo heavyNode, LoadInfo lightNode) {
        if (callBacks != null)
            for (GlobalLoadListener callBack : callBacks) {
                callBack.onOverLoad(heavyNode, lightNode);
            }
    }
}
