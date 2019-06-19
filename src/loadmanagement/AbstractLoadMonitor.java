package loadmanagement;

import commonmodels.LoadChangeHandler;
import commonmodels.LoadChangeListener;
import commonmodels.NotableLoadChangeCallback;
import util.Config;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractLoadMonitor  implements LoadChangeListener {

    protected List<NotableLoadChangeCallback> callbacks;

    protected final LoadChangeHandler handler;

    protected final LoadInfoManager loadInfoManager;

    protected final long lbUpperBound;

    protected final long lbLowerBound;

    public AbstractLoadMonitor(final LoadChangeHandler handler, final LoadInfoManager loadInfoManager) {
        callbacks = new ArrayList<>();
        lbUpperBound = Config.getInstance().getLoadBalancingUpperBound();
        lbLowerBound = Config.getInstance().getLoadBalancingLowerBound();
        this.handler = handler;
        this.loadInfoManager = loadInfoManager;
    }

    public void subscribe(NotableLoadChangeCallback callBack) {
        callbacks.add(callBack);
    }

    public void unsubscribe(NotableLoadChangeCallback callBack) {
        callbacks.remove(callBack);
    }

}