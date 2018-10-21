package algorithms.replciaplacement;

import models.BucketNode;
import models.Indexable;
import models.LookupTable;
import models.PhysicalNode;

import java.util.ArrayList;
import java.util.List;

public class BucketReplicaAlgorithm implements ReplicaPlacementAlgorithm {
    @Override
    public List<PhysicalNode> getReplicas(LookupTable table, Indexable node) {
        List<PhysicalNode> replicas = new ArrayList<>();

        BucketNode bucketNode = (BucketNode) table.getTable().find(node);
        for (String physicalNodeId : bucketNode.getpPhysicalNodes()) {
            replicas.add(table.getPhysicalNodeMap().get(physicalNodeId));
        }

        return replicas;
    }

    @Override
    public List<PhysicalNode> getReplicas(LookupTable table, int hash) {
        return getReplicas(table, new BucketNode(hash));
    }
}
