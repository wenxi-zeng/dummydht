package commonmodels;

import loadmanagement.LoadInfo;

public interface GlobalLoadListener {
    void onOverload(LoadInfo loadInfo);
    void onOverLoad(LoadInfo heavyNode, LoadInfo lightNode);
}
