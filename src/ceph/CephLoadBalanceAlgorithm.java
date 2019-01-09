package ceph;

import commonmodels.Clusterable;
import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import filemanagement.FileTransferManager;
import util.Config;
import util.SimpleLog;

import java.util.*;
import java.util.stream.Collectors;

import static util.Config.STATUS_ACTIVE;
import static util.Config.STATUS_INACTIVE;

public class CephLoadBalanceAlgorithm {

    public void loadBalancing(ClusterMap map, Clusterable clusterable) {
        List<Tuple> transferTupleList = new ArrayList<>();
        List<Clusterable> leaves = clusterable.getLeaves();

        // This is for single node test, thus we have to iterate every physical node.
        // In realistic solution, iteration is not needed, since the content of the
        // the loop will run in each individual data node.
        for (Clusterable leaf : leaves) {
            if (leaf.getStatus().equals(STATUS_INACTIVE)) continue;
            PhysicalNode pnode = (PhysicalNode) leaf;

            // The content from here is actual load balancing
            // that will be run in each data node.

            // Create a transfer list for batch processing.
            Map<String, List<Indexable>> transferList = new HashMap<>();

            // Iterate each placement group
            for (Indexable placementGroup : pnode.getVirtualNodes()) {
                int r = placementGroup.getIndex();
                PlacementGroup pg = (PlacementGroup) placementGroup;
                Clusterable replica;
                do {
                    replica = map.rush(pg.getId(), r++);
                } while (replica.getStatus().equals(STATUS_INACTIVE));

                // if a placement group is determined that it is not
                // in the current node, we need to transfer it to the replica.
                if (!replica.getId().equals(pnode.getId())) {
                    transferList.computeIfAbsent(replica.getId(), k -> new ArrayList<>());
                    transferList.get(replica.getId()).add(pg);
                }
            }

            // batch processing transfer.
            for (Map.Entry<String, List<Indexable>> replica : transferList.entrySet()) {
                PhysicalNode to = map.getPhysicalNodeMap().get(replica.getKey());
                Tuple tuple = new Tuple(pnode, to, replica.getValue());
                transferTupleList.add(tuple);
            }
        }

        for (Tuple tuple : transferTupleList) {
            tuple.from.getVirtualNodes().removeAll(tuple.placementGroup);
            tuple.to.getVirtualNodes().addAll(tuple.placementGroup);
            requestTransfer(tuple.placementGroup, tuple.from, tuple.to);
        }

        if (map.getLoadBalancingCallBack() != null)
            map.getLoadBalancingCallBack().onFinished();
    }

    public void failureRecovery(ClusterMap map, Clusterable failedNode) {
        // This is for single node test, thus we have to iterate every physical node.
        // In realistic solution, iteration is not needed, since the content of the
        // the loop will run in each individual data node.
        List<Tuple> replicationTupleList = new ArrayList<>();
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
                boolean replicatePG = false;
                PlacementGroup pg = (PlacementGroup) placementGroup;
                Clusterable replica = null;

                while (count < Config.getInstance().getNumberOfReplicas()) {
                    replica = map.rush(pg.getId(), r++);

                    // if replicatePG = false, we need to keep checking if the replica
                    // is the failure node.
                    // otherwise, we keep to loop running to exhaust all the replicas
                    // of PG. then we got the maximum value of r, which will be used
                    // for finding a new replica of the PG.
                    if (!replicatePG && replica.getId().equals(failedNode.getId())) {
                        replicatePG = true;
                    }
                    // ignore the failure nodes
                    if (replica.getStatus().equals(STATUS_ACTIVE) && !replica.getId().equals(failedNode.getId())){
                        count++;
                    }
                }

                // if this PG has been determined that it has replica in the failure node,
                // find a new replica for it.
                if (replicatePG) {
                    // we need this do-while loop to make sure the new replica is active and not itself.
                    while (replica.getStatus().equals(STATUS_INACTIVE) || replica.getId().equals(pnode.getId())) {
                        replica = map.rush(pg.getId(), r++);
                    }

                    // add the replica to replication list, we will copy the
                    // placement group to it later.
                    replicationList.computeIfAbsent(replica.getId(), k -> new ArrayList<>());
                    replicationList.get(replica.getId()).add(new PlacementGroup(pg.getId(), r - 1));
                }
            }

            // batch processing replications.
            for (Map.Entry<String, List<Indexable>> replica : replicationList.entrySet()) {
                PhysicalNode to = map.getPhysicalNodeMap().get(replica.getKey());
                Tuple tuple = new Tuple(pnode, to, replica.getValue());
                replicationTupleList.add(tuple);
            }
        }

        for (Tuple tuple : replicationTupleList) {
            Set<Indexable> merged = new HashSet<>(tuple.placementGroup);
            merged.addAll(tuple.to.getVirtualNodes());
            tuple.to.setVirtualNodes(new ArrayList<>(merged));
            requestReplication(tuple.placementGroup, tuple.from, tuple.to);
        }

        if (map.getLoadBalancingCallBack() != null)
            map.getLoadBalancingCallBack().onFinished();
    }

    public void changeWeight(ClusterMap map, PhysicalNode node, float deltaWeight) {
        SimpleLog.i("Changing weight for physical node " + node.toString());

        PhysicalNode pnode = map.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }
        else if (pnode.getStatus().equals(STATUS_INACTIVE)) {
            SimpleLog.i(node.getId() + " has been removed or marked as failure");
            return;
        }

        map.getWeightDistributeStrategy().onWeightChanged(map, pnode, deltaWeight);
        loadBalancing(map, map.getRoot());
        SimpleLog.i("Weight updated. deltaWeight="  + deltaWeight + ", new weight=" + pnode.getWeight());

        if (map.getLoadBalancingCallBack() != null)
            map.getLoadBalancingCallBack().onFinished();
    }

    private void requestTransfer(List<Indexable> placementGroups, PhysicalNode fromNode, PhysicalNode toNode) {
        StringBuilder result = new StringBuilder();

        for (Indexable pg : placementGroups)
            result.append(pg.getDisplayId()).append(' ');

        SimpleLog.i("Transfer placement groups: " + result.toString() + "from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().requestTransfer(
                placementGroups.stream().map(Indexable::getIndex).collect(Collectors.toList()),
                fromNode,
                toNode);
    }

    private void requestReplication(List<Indexable> placementGroups, PhysicalNode fromNode, PhysicalNode toNode) {
        StringBuilder result = new StringBuilder();

        for (Indexable pg : placementGroups)
            result.append(pg.getDisplayId()).append(' ');

        SimpleLog.i("Copy placement groups:" + result.toString() + "from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().requestCopy(
                placementGroups.stream().map(Indexable::getIndex).collect(Collectors.toList()),
                fromNode,
                toNode);
    }

    // Private class
    // A compromise of single node computation
    // Not necessary for realistic case
    private class Tuple {
        PhysicalNode from;
        PhysicalNode to;
        List<Indexable> placementGroup;

        public Tuple(PhysicalNode from, PhysicalNode to, List<Indexable> placementGroup) {
            this.from = from;
            this.to = to;
            this.placementGroup = placementGroup;
        }
    }
}
