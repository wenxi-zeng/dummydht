package commonmodels;

import ceph.CephDecentLoadChangeHandler;
import ceph.CephLoadChangeHandler;
import elastic.ElasticDecentLoadChangeHandler;
import elastic.ElasticLoadChangeHandler;
import ring.RingDecentLoadChangeHandler;
import ring.RingLoadChangeHandler;
import util.Config;

public class LoadChangeHandlerFactory {

    public LoadChangeHandler getHandler(String mode, String scheme, Object table) {
        if (mode.equals(Config.MODE_CENTRIALIZED)) {
            return getGlobalLoadChangeHandler(scheme, table);
        }
        else {
            return getDecentLoadChangeHandler(scheme, table);
        }
    }

    private LoadChangeHandler getGlobalLoadChangeHandler(String scheme, Object table) {
        if (scheme.equals(Config.SCHEME_ELASTIC)) {
            return new ElasticLoadChangeHandler((elastic.LookupTable) table);
        }
        else if (scheme.equals(Config.SCHEME_CEPH)) {
            return new CephLoadChangeHandler((ceph.ClusterMap) table);
        }
        else {
            return new RingLoadChangeHandler((ring.LookupTable) table);
        }
    }

    private LoadChangeHandler getDecentLoadChangeHandler(String scheme, Object table) {
        if (scheme.equals(Config.SCHEME_ELASTIC)) {
            return new ElasticDecentLoadChangeHandler((elastic.LookupTable) table);
        }
        else if (scheme.equals(Config.SCHEME_CEPH)) {
            return new CephDecentLoadChangeHandler((ceph.ClusterMap) table);
        }
        else {
            return new RingDecentLoadChangeHandler((ring.LookupTable) table);
        }
    }
}
