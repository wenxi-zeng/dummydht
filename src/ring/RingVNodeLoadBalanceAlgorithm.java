package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import filemanagement.FileTransferManager;
import util.SimpleLog;

import java.util.UUID;

public class RingVNodeLoadBalanceAlgorithm extends RingLoadBalanceAlgorithm {

    public void moveVNode(LookupTable lookupTable, VirtualNode node, PhysicalNode from, PhysicalNode to) {
        SimpleLog.i("Moving vnode [" + node.getHash() + "] from " + from.getId() + " to " + to.getId());

        node = (VirtualNode) lookupTable.getTable().findNode(node);
        PhysicalNode fromNode = lookupTable.getPhysicalNodeMap().get(from.getId());
        if (fromNode == null) {
            SimpleLog.i(from.getId() + " does not exist.");
            return;
        }
        else if (!fromNode.getVirtualNodes().contains(node)){
            SimpleLog.i(from.getId() + " does not have vnode [" + node.getHash() + "]");
            return;
        }

        PhysicalNode toNode = lookupTable.getPhysicalNodeMap().get(to.getId());
        if (toNode == null) {
            SimpleLog.i(to.getId() + " does not exist.");
            return;
        }
        else if (toNode.getVirtualNodes().contains(node)){
            SimpleLog.i(to.getId() + " already have vnode [" + node.getHash() + "]");
            return;
        }

        fromNode.getVirtualNodes().remove(node);
        toNode.getVirtualNodes().add(node);
        node.setPhysicalNodeId(toNode.getId());

        String token = UUID.randomUUID().toString();

        Indexable predecessor = lookupTable.getTable().pre(node);
        requestTransfer(predecessor.getHash(), node.getHash(), from ,to, token);

        SimpleLog.i("Moving bucket [" + node.getHash() + "] from " + from.getId() + " to " + to.getId());
        SimpleLog.i("Updated bucket info: " + node.toString());
        SimpleLog.i("Updated " + fromNode.getId() + " info: " + fromNode.toString());
        SimpleLog.i("Updated " + toNode.getId() + " info: " + toNode.toString());
    }

    private void requestTransfer(int hi, int hf, PhysicalNode fromNode, PhysicalNode toNode, String token) {
        SimpleLog.i("Request to transfer hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
        FileTransferManager.getInstance().setTransferToken(token);
        FileTransferManager.getInstance().requestTransfer(hi, hf, fromNode, toNode);
    }
}
