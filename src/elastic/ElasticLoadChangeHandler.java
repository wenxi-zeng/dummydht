package elastic;

import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.List;

public class ElasticLoadChangeHandler implements LoadChangeHandler {

    private final LookupTable table;

    public ElasticLoadChangeHandler(LookupTable table) {
        this.table = table;
    }

    @Override
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        return null;
    }
}
