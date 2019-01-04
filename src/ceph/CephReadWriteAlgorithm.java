package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import util.Config;

import java.util.ArrayList;
import java.util.List;

import static util.Config.STATUS_ACTIVE;

public class CephReadWriteAlgorithm {
    public List<PhysicalNode> lookup(ClusterMap clusterMap, String filename) {
        String pgid = clusterMap.getPlacementGroupId(filename);
        int r = 0;

        List<PhysicalNode> pnodes = new ArrayList<>();
        while (pnodes.size() < Config.getInstance().getNumberOfReplicas()) {
            Clusterable node = clusterMap.rush(pgid, r++);

            if (node != null && node.getStatus().equals(STATUS_ACTIVE)) {
                pnodes.add((PhysicalNode) node);
            }
        }

        return pnodes;
    }
}
