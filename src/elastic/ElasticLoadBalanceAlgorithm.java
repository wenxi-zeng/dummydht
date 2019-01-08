package elastic;

import commonmodels.PhysicalNode;
import filemanagement.FileTransferManager;
import util.Config;
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

        if (lookupTable.getLoadBalancingCallBack() != null)
            lookupTable.getLoadBalancingCallBack().onFinished();
    }

    public void copyBucket(LookupTable lookupTable, BucketNode node, PhysicalNode to) {
        Random random = new Random();
        int index = random.nextInt(node.getPhysicalNodes().size());
        requestReplication(node,
                lookupTable.getPhysicalNodeMap().get(node.getPhysicalNodes().get(index)),
                to);

        if (lookupTable.getLoadBalancingCallBack() != null)
            lookupTable.getLoadBalancingCallBack().onFinished();
    }

    public void transferBucket(LookupTable lookupTable, BucketNode node, PhysicalNode to) {
        Random random = new Random();
        int index = random.nextInt(node.getPhysicalNodes().size());
        requestTransfer(node,
                lookupTable.getPhysicalNodeMap().get(node.getPhysicalNodes().get(index)),
                to);
        node.getPhysicalNodes().remove(index);
        node.getPhysicalNodes().add(to.getId());

        if (lookupTable.getLoadBalancingCallBack() != null)
            lookupTable.getLoadBalancingCallBack().onFinished();
    }

    public void onTableExpand(LookupTable table) {
        SimpleLog.i("Expanding table...");

        int originalSize = table.getTable().length;
        table.expandTable();

        for (int i = 0; i< originalSize; i++) {
            table.getTable()[originalSize + i].getPhysicalNodes().addAll(table.getTable()[i].getPhysicalNodes());
        }

        SimpleLog.i("Table expanded. No file transfer needed");

        if (table.getLoadBalancingCallBack() != null)
            table.getLoadBalancingCallBack().onFinished();
    }

    public void onTableShrink(LookupTable table) {
        SimpleLog.i("Shrinking table...");

        if (table.getTable().length  / 2 < Config.getInstance().getDefaultNumberOfHashSlots()) {
            SimpleLog.i("Table cannot be shrunk anymore");
            return;
        }
        int newSize = table.getTable().length / 2;

        for (int i = 0; i< newSize; i++) {
            for (String nodeId : table.getTable()[newSize + i].getPhysicalNodes()) {
                if (table.getTable()[i].getPhysicalNodes().contains(nodeId)) continue;

                for (String targetId : table.getTable()[i].getPhysicalNodes()) {
                    requestTransfer(table.getTable()[newSize + i],
                            table.getPhysicalNodeMap().get(nodeId),
                            table.getPhysicalNodeMap().get(targetId));
                }
            }
        }

        table.shrinkTable();
        SimpleLog.i("Table shrank.");

        if (table.getLoadBalancingCallBack() != null)
            table.getLoadBalancingCallBack().onFinished();
    }

    private void requestTransfer(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Request to transfer hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().requestTransfer(node.getHash(), fromNode, toNode);
    }

    private void requestReplication(BucketNode node, PhysicalNode fromNode, PhysicalNode toNode) {
        SimpleLog.i("Copy hash bucket [" + node.getHash() + "] from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().requestCopy(node.getHash(), fromNode, toNode);
    }

}
