package commonmodels;

import ceph.CephDecentLoadChangeHandler;
import ceph.CephLoadChangeHandler;
import elastic.ElasticDecentLoadChangeHandler;
import elastic.ElasticLoadChangeHandler;
import ring.RingDecentLoadChangeHandler;
import ring.RingDecentVNodeLoadChangeHandler;
import ring.RingLoadChangeHandler;
import ring.RingVNodeLoadChangeHandler;
import util.Config;

public class LoadChangeHandlerFactory {

    public LoadChangeHandler getHandler(String mode, String scheme, boolean useVNode, Object table) {
        if (mode.equals(Config.MODE_CENTRIALIZED)) {
            return getGlobalLoadChangeHandler(scheme, useVNode, table);
        }
        else {
            return getDecentLoadChangeHandler(scheme, useVNode, table);
        }
    }

    private LoadChangeHandler getGlobalLoadChangeHandler(String scheme, boolean useVNode, Object table) {
        if (scheme.equals(Config.SCHEME_ELASTIC)) {
            return new ElasticLoadChangeHandler((elastic.LookupTable) table);
        }
        else if (scheme.equals(Config.SCHEME_CEPH)) {
            return new CephLoadChangeHandler((ceph.ClusterMap) table);
        }
        else {
            if (useVNode)
                return new RingVNodeLoadChangeHandler((ring.LookupTable) table);
            else
                return new RingLoadChangeHandler((ring.LookupTable) table);
        }
    }

    private LoadChangeHandler getDecentLoadChangeHandler(String scheme, boolean useVNode, Object table) {
        if (scheme.equals(Config.SCHEME_ELASTIC)) {
            return new ElasticDecentLoadChangeHandler((elastic.LookupTable) table);
        }
        else if (scheme.equals(Config.SCHEME_CEPH)) {
            return new CephDecentLoadChangeHandler((ceph.ClusterMap) table);
        }
        else {
            if (useVNode)
                return new RingDecentVNodeLoadChangeHandler((ring.LookupTable) table);
            else
                return new RingDecentLoadChangeHandler((ring.LookupTable) table);
        }
    }
}
