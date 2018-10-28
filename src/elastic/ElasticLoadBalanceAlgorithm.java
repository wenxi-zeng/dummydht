package elastic;

import commonmodels.PhysicalNode;
import filemanagement.FileTransferManager;
import util.SimpleLog;

import java.util.Random;

public class ElasticLoadBalanceAlgorithm {

    public void moveBucket(LookupTable lookupTable, BucketNode node, PhysicalNode from, PhysicalNode to) {
        SimpleLog.i("Moving bucket [" + node.getHash() + "] from " + from.getId() + " to " + to.getId());

        PhysicalNode fromNode = lookupTable.getPhysicalNodeMap().get(from.getId());
        if (fromNode == null) {
            SimpleLog.i(from.getId() + " does not exist.");
            return;
        }
        else if (!fromNode.getVirtualNodes().contains(node)){
            SimpleLog.i(from.getId() + " does not have bucket [" + node.getHash() + "]");
            return;
        }

        PhysicalNode toNode = lookupTable.getPhysicalNodeMap().get(to.getId());
        if (toNode == null) {
            SimpleLog.i(to.getId() + " does not exist.");
            return;
        }
        else if (toNode.getVirtualNodes().contains(node)){
            SimpleLog.i(to.getId() + " already have bucket [" + node.getHash() + "]");
            return;
        }

        node = lookupTable.getTable()[node.getHash()];
        node.getPhysicalNodes().remove(fromNode.getId());
        fromNode.getVirtualNodes().remove(node);
        node.getPhysicalNodes().add(toNode.getId());
        toNode.getVirtualNodes().add(node);

        requestTransfer(node, from ,to);

        SimpleLog.i("Moving bucket [" + node.getHash() + "] from " + from.getId() + " to " + to.getId());
        SimpleLog.i("Updated bucket info: " + node.toString());
        SimpleLog.i("Updated " + fromNode.getId() + " info: " + fromNode.toString());
        SimpleLog.i("Updated " + toNode.getId() + " info: " + toNode.toString());
    }

    public void copyBucket(LookupTable lookupTable, BucketNode node, PhysicalNode to) {
        Random random = new Random();
        int index = random.nextInt(node.getPhysicalNodes().size());
        requestReplication(node,
                lookupTable.getPhysicalNodeMap().get(node.getPhysicalNodes().get(index)),
                to);
    }

    private void transfer(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Transfer hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().transfer(node.getHash(), fromNode, toNode);
    }

    private void requestTransfer(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Request to transfer hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().transfer(node.getHash(), fromNode, toNode);
    }

    private void requestReplication(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Copy hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().copy(node.getHash(), fromNode, toNode);
    }

}
