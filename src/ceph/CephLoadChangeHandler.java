package ceph;

import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.List;

public class CephLoadChangeHandler implements LoadChangeHandler {

    private final ClusterMap table;

    public CephLoadChangeHandler(ClusterMap table) {
        this.table = table;
    }

    @Override
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        return null;
    }
}
