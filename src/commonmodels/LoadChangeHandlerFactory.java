package commonmodels;

import ceph.CephDecentLoadChangeHandler;
import ceph.CephLoadChangeHandler;
import elastic.ElasticDecentLoadChangeHandler;
import elastic.ElasticLoadChangeHandler;
import ring.*;
import util.Config;

public class LoadChangeHandlerFactory {

    public LoadChangeHandler getHandler(String mode, String scheme, String algo, Object table) {
        if (mode.equals(Config.MODE_CENTRIALIZED)) {
            return getGlobalLoadChangeHandler(scheme, algo, table);
        }
        else {
            return getDecentLoadChangeHandler(scheme, algo, table);
        }
    }

    private LoadChangeHandler getGlobalLoadChangeHandler(String scheme, String algo, Object table) {
        if (scheme.equals(Config.SCHEME_ELASTIC)) {
            return new ElasticLoadChangeHandler((elastic.LookupTable) table);
        }
        else if (scheme.equals(Config.SCHEME_CEPH)) {
            return new CephLoadChangeHandler((ceph.ClusterMap) table);
        }
        else {
            if (algo.equals(Config.RING_LB_ALGO_VNODE))
                return new RingVNodeLoadChangeHandler((ring.LookupTable) table);
            else if (algo.equals(Config.RING_LB_ALGO_FORWARD))
                return new RingForwardLoadChangeHandler((ring.LookupTable) table);
            else
                return new RingLoadChangeHandler((ring.LookupTable) table);
        }
    }

    private LoadChangeHandler getDecentLoadChangeHandler(String scheme, String algo, Object table) {
        if (scheme.equals(Config.SCHEME_ELASTIC)) {
            return new ElasticDecentLoadChangeHandler((elastic.LookupTable) table);
        }
        else if (scheme.equals(Config.SCHEME_CEPH)) {
            return new CephDecentLoadChangeHandler((ceph.ClusterMap) table);
        }
        else {
            if (algo.equals(Config.RING_LB_ALGO_VNODE))
                return new RingDecentVNodeLoadChangeHandler((ring.LookupTable) table);
            else if (algo.equals(Config.RING_LB_ALGO_FORWARD))
                return new RingDecentForwardLoadChangeHandler((ring.LookupTable) table);
            else
                return new RingDecentLoadChangeHandler((ring.LookupTable) table);
        }
    }
}
