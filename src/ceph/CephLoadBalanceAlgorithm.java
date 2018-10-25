package ceph;

import commonmodels.Clusterable;
import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static util.Config.NUMBER_OF_REPLICAS;
import static util.Config.STATUS_ACTIVE;
import static util.Config.STATUS_INACTIVE;

public class CephLoadBalanceAlgorithm {

    public void loadBalancing(ClusterMap map, Clusterable clusterable) {

    }

    public void failureRecovery(ClusterMap map, Clusterable failedNode) {
        // This is for single node test, thus we have to iterate every physical node.
        // In realistic solution, iteration is not needed, since the content of the
        // the loop will run in each individual data node.
        for (Map.Entry<String, PhysicalNode> entry : map.getPhysicalNodeMap().entrySet()) {
            PhysicalNode pnode = entry.getValue();
            if (pnode.getStatus().equals(STATUS_INACTIVE)) continue;

            // The content from here is the actual failure handling
            // that will be run in each data node.

            // Create a replication list for batch processing.
            Map<String, List<Indexable>> replicationList = new HashMap<>();

            // Iterate each placement group
            for (Indexable placementGroup : pnode.getVirtualNodes()) {
                int count = 0;
                int r = 0;
                PlacementGroup pg = (PlacementGroup) placementGroup;

                while (count < NUMBER_OF_REPLICAS) {
                    Clusterable repilica = map.rush(pg.getId(), r++);

                    // if a placement group is determined that it is located
                    // in the failure node, we need to find a new replica for it.
                    if (repilica.getId().equals(failedNode.getId())) {
                        do {
                            repilica = map.rush(pg.getId(), r++);
                        }
                        while (!repilica.getStatus().equals(STATUS_ACTIVE));

                        // add the replica to replication list, we will copy the
                        // placement group to it later.
                        replicationList.computeIfAbsent(repilica.getId(), k -> new ArrayList<>());
                        replicationList.get(repilica.getId()).add(pg);
                        break;
                    }

                    count++;
                }
            }

            // batch processing replications.
            for (Map.Entry<String, List<Indexable>> replica : replicationList.entrySet()) {
                requestReplication(replica.getValue(), pnode, map.getPhysicalNodeMap().get(replica.getKey()));
            }
        }
    }

    private void requestReplication(List<Indexable> placementGroups, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Copy placement groups:\n" + placementGroups.toString() + "from " + fromNode.toString() + " to " + toNode.toString());
    }
}
