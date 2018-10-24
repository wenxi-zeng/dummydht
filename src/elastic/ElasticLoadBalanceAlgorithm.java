package elastic;

import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.Random;

public class ElasticLoadBalanceAlgorithm {

    public void moveBucket(LookupTable lookupTable, BucketNode node, PhysicalNode from, PhysicalNode to) {
        requestTransfer(node, from ,to);
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
    }

    private void requestTransfer(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Request to transfer hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
    }

    private void requestReplication(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Copy hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
    }

}
