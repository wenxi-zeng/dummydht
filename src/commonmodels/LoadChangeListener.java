package commonmodels;

import loadmanagement.LoadInfo;

import java.util.List;

public interface LoadChangeListener {
    void onLoadUpdated(List<LoadInfo> loadInfoList);
}
