package loadmanagement;

import commonmodels.GlobalLoadListener;
import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import util.Config;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;

public class LoadMonitor implements GlobalLoadListener {

    private List<NotableLoadChangeCallback> callbacks;

    private LoadChangeHandler handler;

    private final long lbUpperBound;

    private final long lbLowerBound;

    public LoadMonitor(final  LoadChangeHandler handler) {
        callbacks = new ArrayList<>();
        lbUpperBound = Config.getInstance().getLoadBalancingUpperBound();
        lbLowerBound = Config.getInstance().getLoadBalancingLowerBound();
        this.handler = handler;
    }

    public void subscribe(NotableLoadChangeCallback callBack) {
        callbacks.add(callBack);
    }

    public void unsubscribe(NotableLoadChangeCallback callBack) {
        callbacks.remove(callBack);
    }

    @Override
    public void onLoadUpdated(List<LoadInfo> globalLoad) {
        boolean allFull = true;
        for (LoadInfo info : globalLoad) {
            if (info.getFileLoad() < lbLowerBound) {
                allFull = false;
                break;
            }
        }

        if (allFull) {
            SimpleLog.v("All nodes are higher than load balancing lower bound, no need to balance");
        }
        else {
            for (LoadInfo info : globalLoad) {
                if (info.getSizeOfFiles() > lbUpperBound) {
                    SimpleLog.v("Node " + info.getNodeId() + " is overloaded. Decreasing its load");
                    onOverload(globalLoad, info);
                }
            }
        }
    }

    private void onOverload(List<LoadInfo> globalLoad, LoadInfo loadInfo) {
        Request request = handler.generateRequestBasedOnLoad(globalLoad, loadInfo, lbLowerBound, lbUpperBound);

        for (NotableLoadChangeCallback callback : callbacks)
            callback.onRequestAvailable(request);
    }

    public interface NotableLoadChangeCallback {
        void onRequestAvailable(Request request);
    }
}
