package algorithms;

import models.*;

import java.util.ArrayList;
import java.util.List;

public class RingReplicaAlgorithm implements ReplicaPlacementAlgorithm {

    public final static int NUMBER_OF_REPLICAS = 3;

    @Override
    public List<PhysicalNode> getReplicas(LookupTable table, Indexable node) {
        List<PhysicalNode> replicas = new ArrayList<>();

        VirtualNode replica = (VirtualNode) table.getTable().find(node);
        replicas.add(table.getPhysicalNodeMap().get(replica.getPhysicalNodeId()));

        for (int i = 0; i < NUMBER_OF_REPLICAS - 1; i++) {
            replica = (VirtualNode) table.getTable().next(replica);
            replicas.add(table.getPhysicalNodeMap().get(replica.getPhysicalNodeId()));
        }

        return replicas;
    }

    @Override
    public List<PhysicalNode> getReplicas(LookupTable table, int hash) {
        return getReplicas(table, new VirtualNode(hash));
    }
}
