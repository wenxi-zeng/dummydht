package ceph;

import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.List;

public class CephLoadChangeHandler implements LoadChangeHandler {
    @Override
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        return null;
    }
}
