package loadmanagement;

import commonmodels.LoadChangeListener;

import java.util.ArrayList;
import java.util.List;

public abstract class LoadInfoBroker {

    protected List<LoadChangeListener> callBacks = new ArrayList<>();

    public void subscribe(LoadChangeListener callBack) {
        callBacks.add(callBack);
    }

    public void unsubscribe(LoadChangeListener callBack) {
        callBacks.remove(callBack);
    }

    public void announce(List<LoadInfo> infoList) {
        for (LoadChangeListener callBack : callBacks) {
            callBack.onLoadUpdated(infoList);
        }
    }
}
