package commonmodels;

import loadmanagement.LoadInfo;

import java.util.List;

public interface GlobalLoadListener {
    void onLoadUpdated(List<LoadInfo> globalLoad);
}
