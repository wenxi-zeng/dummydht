package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import filemanagement.FileTransferManager;
import util.Config;
import util.MathX;
import util.SimpleLog;

public class RingLoadBalanceAlgorithm {

    public void increaseLoad(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Increasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (Indexable vnode : pnode.getVirtualNodes()) {
            Indexable successor = table.getTable().next(vnode);
            if (successor == null) {
                SimpleLog.i("Virtual node [hash=" + vnode.getHash() + "] is no longer valid");
                continue;
            }

            int bound = successor.getHash() - vnode.getHash();
            int dh = MathX.nextInt(bound < 0 ? Config.getInstance().getNumberOfHashSlots() + bound : bound);
            SimpleLog.i("Increasing load for virtual node of " + node.toString() + ", delta h=" + dh);
            increaseLoad(table, dh, vnode);
        }
    }

    public void increaseLoad(LookupTable table, PhysicalNode node, int[] hashVal) {
        SimpleLog.i("Increasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            increaseLoad(table, hashVal[i], vnode);
        }
    }

    public void decreaseLoad(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Decreasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (Indexable vnode : pnode.getVirtualNodes()) {
            Indexable predecessor = table.getTable().pre(vnode);
            if (predecessor == null) {
                SimpleLog.i("Virtual node [hash=" + vnode.getHash() + "] is no longer valid");
                continue;
            }

            int bound = vnode.getHash() - predecessor.getHash();
            int dh = MathX.nextInt(bound < 0 ? Config.getInstance().getNumberOfHashSlots() + bound : bound);
            SimpleLog.i("Decreasing load for virtual node of " + node.toString() + ", delta h=" + dh);
            decreaseLoad(table, dh, vnode);
        }
    }

    public void decreaseLoad(LookupTable table, PhysicalNode node, int[] hashVal) {
        SimpleLog.i("Decreasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            decreaseLoad(table, hashVal[i], vnode);
        }
    }

    public int[] randomIncreaseRange(LookupTable table, PhysicalNode node) {
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        int[] hashVals = new int[pnode.getVirtualNodes().size()];

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            Indexable successor = table.getTable().next(vnode);
            if (successor == null) {
                SimpleLog.i("Virtual node [hash=" + vnode.getHash() + "] is no longer valid");
                continue;
            }

            int bound = successor.getHash() - vnode.getHash();
            hashVals[i] = MathX.nextInt(bound < 0 ? Config.getInstance().getNumberOfHashSlots() + bound : bound);
        }

        return hashVals;
    }

    public int[] randomDecreaseRange(LookupTable table, PhysicalNode node) {
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        int[] hashVals = new int[pnode.getVirtualNodes().size()];

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            Indexable predecessor = table.getTable().pre(vnode);
            if (predecessor == null) {
                SimpleLog.i("Virtual node [hash=" + vnode.getHash() + "] is no longer valid");
                continue;
            }

            int bound = vnode.getHash() - predecessor.getHash();
            hashVals[i] = MathX.nextInt(bound < 0 ? Config.getInstance().getNumberOfHashSlots() + bound : bound);
        }

        return hashVals;
    }

    public void decreaseLoad(LookupTable table, int dh, Indexable node) {
        int hi = node.getHash();
        int hf = hi - dh;
        if (hf < 0) hf = Config.getInstance().getNumberOfHashSlots() + hf;

        Indexable predecessor = table.getTable().pre(node);
        if (predecessor == null) {
            SimpleLog.i("Virtual node [hash=" + node.getHash() + "] is no longer valid");
            return;
        }
        if (dh == 0 || hf == predecessor.getHash()) {
            SimpleLog.i("Invalid hash change, delta h=" + dh);
            return;
        }

        Indexable toNode = table.getTable().get(node.getIndex() + Config.getInstance().getNumberOfReplicas());
        requestTransfer(table, hf, hi, node, toNode);   // transfer(start, end, from, to). (start, end]

        node.setHash(hf);
        SimpleLog.i("Decreased load for virtual node " + hi + " to " + hf);
        SimpleLog.i("Updated node info: " + node.toString());
    }

    public void increaseLoad(LookupTable table, int dh, Indexable node) {
        int hi = node.getHash();
        int hf = (hi + dh) % Config.getInstance().getNumberOfHashSlots();

        Indexable successor = table.getTable().next(node);
        if (successor == null) {
            SimpleLog.i("Virtual node [hash=" + node.getHash() + "] is no longer valid");
            return;
        }
        if (dh == 0 || hf == successor.getHash()) {
            SimpleLog.i("Invalid hash change, delta h=" + dh);
            return;
        }

        Indexable fromNode = table.getTable().get(node.getIndex() + Config.getInstance().getNumberOfReplicas());
        requestTransfer(table, hi, hf, fromNode, node); // requestTransfer(start, end, from, to). (start, end]

        node.setHash(hf);
        SimpleLog.i("Increased load for virtual node " + hi + " to " + hf);
        SimpleLog.i("Updated node info: " + node.toString());
    }

    public void nodeJoin(LookupTable table, Indexable node) {
        SimpleLog.i("Adding virtual node [hash=" + node.getHash() + "] for " + ((VirtualNode)node).getPhysicalNodeId());

        Indexable successor = table.getTable().next(node);
        Indexable startNode = table.getTable().get(node.getIndex() - Config.getInstance().getNumberOfReplicas());
        Indexable endNode = table.getTable().next(startNode);

        for (int i = 0; i < Config.getInstance().getNumberOfReplicas(); i++) {
            int hi = startNode.getHash();
            int hf = endNode.getHash();

            requestTransfer(table, hi, hf, successor, node); // requestTransfer(start, end, from, to). (start, end]

            startNode = endNode;
            endNode = table.getTable().next(startNode);
            successor = table.getTable().next(successor);
        }

        SimpleLog.i("Virtual node [hash=" + node.getHash() + "] added");
    }

    public void nodeLeave(LookupTable table, Indexable node) {
        SimpleLog.i("Removing virtual node [hash=" + node.getHash() + "] from " + ((VirtualNode)node).getPhysicalNodeId());

        Indexable successor = table.getTable().get(node.getIndex());
        Indexable predecessor = table.getTable().pre(successor);
        Indexable startNode = table.getTable().get(node.getIndex() - Config.getInstance().getNumberOfReplicas());
        Indexable endNode = table.getTable().next(startNode);

        for (int i = 0; i < Config.getInstance().getNumberOfReplicas(); i++) {
            int hi = startNode.getHash();
            int hf = endNode.getHash();

            requestReplication(table, hi, hf, predecessor, successor); // requestTransfer(start, end, from, to). (start, end]

            startNode = endNode;
            endNode = table.getTable().next(startNode);
            predecessor = successor;
            successor = table.getTable().next(successor);
        }

        SimpleLog.i("Virtual node [hash=" + node.getHash() + "] removed");
    }

    private void requestTransfer(LookupTable table, int hi, int hf, Indexable fromNode, Indexable toNode) {
        SimpleLog.i("Request to transfer hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
        String fromNodeId = ((VirtualNode)fromNode).getPhysicalNodeId();
        String toNodeId = ((VirtualNode)toNode).getPhysicalNodeId();
        FileTransferManager.getInstance().requestTransfer(hi, hf, table.getPhysicalNodeMap().get(fromNodeId), table.getPhysicalNodeMap().get(toNodeId));
    }

    private void requestReplication(LookupTable table, int hi, int hf, Indexable fromNode, Indexable toNode) {
        SimpleLog.i("Copy hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
        String fromNodeId = ((VirtualNode)fromNode).getPhysicalNodeId();
        String toNodeId = ((VirtualNode)toNode).getPhysicalNodeId();
        FileTransferManager.getInstance().requestCopy(hi, hf, table.getPhysicalNodeMap().get(fromNodeId), table.getPhysicalNodeMap().get(toNodeId));
    }
}
