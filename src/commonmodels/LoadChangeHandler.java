package commonmodels;

import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.List;

public interface LoadChangeHandler {
    Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound);
}
