package loadmanagement;

import commonmodels.LoadChangeHandler;
import commonmodels.NotableLoadChangeCallback;
import commonmodels.transport.Request;
import util.SimpleLog;

import java.util.List;

public class DecentralizedLoadMonitor extends AbstractLoadMonitor {

    public DecentralizedLoadMonitor(LoadChangeHandler handler, LoadInfoManager loadInfoManager) {
        super(handler, loadInfoManager);
    }

    @Override
    public void onLoadUpdated(List<LoadInfo> loadInfoList) {
        List<Request> requests = handler.generateRequestBasedOnLoad(loadInfoList, loadInfoManager.getLoadInfo(), lbLowerBound, lbUpperBound);

        handler.optimize(requests);

        if (requests.size() < 1) {
            SimpleLog.i("Failed to auto balance load. No request generated");
            return;
        }

        for (NotableLoadChangeCallback callback : callbacks)
            callback.onRequestAvailable(requests);
    }
}
