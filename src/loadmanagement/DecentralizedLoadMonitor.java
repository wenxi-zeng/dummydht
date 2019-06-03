package loadmanagement;

import commonmodels.LoadChangeHandler;

import java.util.List;

public class DecentralizedLoadMonitor extends AbstractLoadMonitor {

    public DecentralizedLoadMonitor(LoadChangeHandler handler) {
        super(handler);
    }

    @Override
    public void onLoadUpdated(List<LoadInfo> loadInfoList) {

    }
}
