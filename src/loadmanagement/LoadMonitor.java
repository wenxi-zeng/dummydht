package loadmanagement;

import commonmodels.LoadChangeHandler;
import commonmodels.NotableLoadChangeCallback;
import commonmodels.transport.Request;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;

public class LoadMonitor extends AbstractLoadMonitor {

    public LoadMonitor(LoadChangeHandler handler) {
        super(handler, null);
    }

    @Override
    public void onLoadUpdated(List<LoadInfo> globalLoad) {
        boolean allFull = true;
        for (LoadInfo info : globalLoad) {
            if (info.getLoad() < lbLowerBound) {
                allFull = false;
                break;
            }
        }

        if (allFull) {
            SimpleLog.v("All nodes are higher than load balancing lower bound, no need to balance");
        }
        else {
            List<Request> requests = new ArrayList<>();
            for (LoadInfo info : globalLoad) {
                if (info.isLoadBalancing()) {
                    SimpleLog.v("Node " + info.getNodeId() + " is under load balancing, no operation is taken");
                    continue;
                }
                if (info.getLoad() > lbUpperBound) {
                    SimpleLog.v("Node " + info.getNodeId() + " is overloaded. Decreasing its load");
                    requests.addAll(onOverload(globalLoad, info));
                }
            }
            handler.optimize(requests);

            if (requests.size() < 1) {
                SimpleLog.v("Failed to auto balance load. No applicable node found");
                return;
            }

            for (NotableLoadChangeCallback callback : callbacks)
                callback.onRequestAvailable(requests);
        }
    }

    private List<Request> onOverload(List<LoadInfo> globalLoad, LoadInfo loadInfo) {
        List<Request> requests = handler.generateRequestBasedOnLoad(globalLoad, loadInfo, lbLowerBound, lbUpperBound);
        return  requests == null ? new ArrayList<>() : requests;
    }
}
