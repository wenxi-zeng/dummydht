package loadmanagement;

import commonmodels.LoadChangeHandler;
import commonmodels.LoadChangeListener;
import commonmodels.NotableLoadChangeCallback;
import util.Config;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractLoadMonitor  implements LoadChangeListener {

    protected List<NotableLoadChangeCallback> callbacks;

    protected LoadChangeHandler handler;

    protected final long lbUpperBound;

    protected final long lbLowerBound;

    public AbstractLoadMonitor(final LoadChangeHandler handler) {
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

}