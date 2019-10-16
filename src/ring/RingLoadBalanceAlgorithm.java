package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import filemanagement.FileTransferManager;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.util.UUID;

public class RingLoadBalanceAlgorithm {

    public void increaseLoad(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Increasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        String token = UUID.randomUUID().toString();
        for (Indexable vnode : pnode.getVirtualNodes()) {
            Indexable successor = table.getTable().next(vnode);
            if (successor == null) {
                SimpleLog.i("Virtual node [hash=" + vnode.getHash() + "] is no longer valid");
                continue;
            }

            int bound = range(vnode.getHash(), successor.getHash());
            int delta = MathX.nextInt(bound);
            int hf = (vnode.getHash() + delta) % Config.getInstance().getNumberOfHashSlots();
            SimpleLog.i("Increasing load for virtual node of " + node.toString() + ", delta h=" + delta);
            increaseLoad(table, hf, vnode, token);
        }
    }

    public void increaseLoad(LookupTable table, PhysicalNode node, int[] hashVal) {
        SimpleLog.i("Increasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        String token = UUID.randomUUID().toString();
        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            increaseLoad(table, hashVal[i], vnode, token);
        }
    }

    public void decreaseLoad(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Decreasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        String token = UUID.randomUUID().toString();
        for (Indexable vnode : pnode.getVirtualNodes()) {
            Indexable predecessor = table.getTable().pre(vnode);
            if (predecessor == null) {
                SimpleLog.i("Virtual node [hash=" + vnode.getHash() + "] is no longer valid");
                continue;
            }

            int bound = range(predecessor.getHash(), vnode.getHash());
            int delta = MathX.nextInt(bound);
            int hf = vnode.getHash() - delta;
            if (hf < 0) hf = Config.getInstance().getNumberOfHashSlots() + hf;
            SimpleLog.i("Decreasing load for virtual node of " + node.toString() + ", delta h=" + delta);
            decreaseLoad(table, hf, vnode, token);
        }
    }

    public void decreaseLoad(LookupTable table, PhysicalNode node, int[] hashVal) {
        SimpleLog.i("Decreasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        String token = UUID.randomUUID().toString();
        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            decreaseLoad(table, hashVal[i], vnode, token);
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

            int bound = range(vnode.getHash(), successor.getHash());
            int delta = MathX.nextInt(bound);
            int hf = (vnode.getHash() + delta) % Config.getInstance().getNumberOfHashSlots();
            hashVals[i] = hf;
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

            int bound = range(predecessor.getHash(), vnode.getHash());
            int delta = MathX.nextInt(bound);
            int hf = vnode.getHash() - delta;
            if (hf < 0) hf = Config.getInstance().getNumberOfHashSlots() + hf;
            hashVals[i] = hf;
        }

        return hashVals;
    }

    public void decreaseLoad(LookupTable table, int hf, Indexable node, String token) {
        int hi = node.getHash();

        Indexable predecessor = table.getTable().pre(node);
        if (predecessor == null) {
            SimpleLog.i("Virtual node [hash=" + node.getHash() + "] is no longer valid");
            return;
        }
        if (!inRange(hf, predecessor.getHash(), hi)) {
            SimpleLog.i("Invalid hash change, hf=" + hf + " not in range (" + predecessor.getHash() + ", " + hi + ")");
            return;
        }

        Indexable toNode = table.getTable().get(node.getIndex() + Config.getInstance().getNumberOfReplicas() - 1);
        requestTransfer(table, hf, hi, node, toNode, token);   // transfer(start, end, from, to). (start, end]

        // node.setHash(hf);
        table.getTable().get(node.getIndex()).setHash(hf);
        SimpleLog.i("Decreased load for virtual node " + hi + " to " + hf);
        SimpleLog.i("Updated node info: " + node.toString());
    }

    public void increaseLoad(LookupTable table, int hf, Indexable node, String token) {
        int hi = node.getHash();

        Indexable successor = table.getTable().next(node);
        if (successor == null) {
            SimpleLog.i("Virtual node [hash=" + node.getHash() + "] is no longer valid");
            return;
        }
        if (!inRange(hf, hi, successor.getHash())) {
            SimpleLog.i("Invalid hash change, hf=" + hf + " not in range (" + hi + ", " + successor.getHash() + ")");
            return;
        }

        Indexable fromNode = table.getTable().get(node.getIndex() + Config.getInstance().getNumberOfReplicas() - 1);
        requestTransfer(table, hi, hf, fromNode, node, token); // requestTransfer(start, end, from, to). (start, end]

        // node.setHash(hf);
        table.getTable().get(node.getIndex()).setHash(hf);
        SimpleLog.i("Increased load for virtual node " + hi + " to " + hf);
        SimpleLog.i("Updated node info: " + node.toString());
    }

    public void nodeJoin(LookupTable table, Indexable node) {
        SimpleLog.i("Adding virtual node [hash=" + node.getHash() + "] for " + ((VirtualNode)node).getPhysicalNodeId());

        Indexable successor = table.getTable().next(node);
        Indexable startNode = table.getTable().get(node.getIndex() - Config.getInstance().getNumberOfReplicas() + 1);
        Indexable endNode = table.getTable().next(startNode);

        String token = UUID.randomUUID().toString();
        for (int i = 0; i < Config.getInstance().getNumberOfReplicas() - 1; i++) {
            int hi = startNode.getHash();
            int hf = endNode.getHash();

            requestTransfer(table, hi, hf, successor, node, token); // requestTransfer(start, end, from, to). (start, end]

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
        Indexable startNode = table.getTable().get(node.getIndex() - Config.getInstance().getNumberOfReplicas() + 1);
        Indexable endNode = table.getTable().next(startNode);

        for (int i = 0; i < Config.getInstance().getNumberOfReplicas() - 1; i++) {
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

    private boolean inRange(int bucket, int start, int end) {
        if (start > end) {
            return (bucket > start && bucket < Config.getInstance().getNumberOfHashSlots()) ||
                    (bucket >= 0 && bucket < end);
        }
        else {
            return bucket > start && bucket < end;
        }
    }

    private int range(int start, int end) {
        if (start > end) {
            return end - start;
        }
        else {
            int bound = Config.getInstance().getNumberOfHashSlots();
            return bound - start + end;
        }
    }

    private void requestTransfer(LookupTable table, int hi, int hf, Indexable fromNode, Indexable toNode, String token) {
        SimpleLog.i("Request to transfer hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
        String fromNodeId = ((VirtualNode)fromNode).getPhysicalNodeId();
        String toNodeId = ((VirtualNode)toNode).getPhysicalNodeId();
        FileTransferManager.getInstance().setTransferToken(token);
        FileTransferManager.getInstance().requestTransfer(hi, hf, table.getPhysicalNodeMap().get(fromNodeId), table.getPhysicalNodeMap().get(toNodeId));
    }

    private void requestReplication(LookupTable table, int hi, int hf, Indexable fromNode, Indexable toNode) {
        SimpleLog.i("Copy hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
        String fromNodeId = ((VirtualNode)fromNode).getPhysicalNodeId();
        String toNodeId = ((VirtualNode)toNode).getPhysicalNodeId();
        FileTransferManager.getInstance().requestCopy(hi, hf, table.getPhysicalNodeMap().get(fromNodeId), table.getPhysicalNodeMap().get(toNodeId));
    }
}
