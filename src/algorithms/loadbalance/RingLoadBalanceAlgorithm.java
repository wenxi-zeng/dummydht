package algorithms.loadbalance;

import models.Indexable;
import models.LookupTable;
import models.PhysicalNode;
import models.VirtualNode;
import util.MathX;
import util.SimpleLog;

import static util.Config.NUMBER_OF_HASH_SLOTS;
import static util.Config.NUMBER_OF_REPLICAS;

public class RingLoadBalanceAlgorithm implements LoadBalanceAlgorithm {
    @Override
    public void increaseLoad(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Increasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (Indexable vnode : pnode.getVirtualNodes()) {
            Indexable successor = table.getTable().next(vnode);
            int bound = successor.getHash() - vnode.getHash();
            int dh = MathX.NextInt(bound < 0 ? NUMBER_OF_HASH_SLOTS + bound : bound);

            SimpleLog.i("Increasing load for virtual node of " + node.toString() + ", delta h=" + dh);
            increaseLoad(table, dh, vnode);
        }
    }

    @Override
    public void decreaseLoad(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Decreasing load for physical node " + node.toString());

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (Indexable vnode : pnode.getVirtualNodes()) {
            Indexable predecessor = table.getTable().pre(vnode);
            int bound = vnode.getHash() - predecessor.getHash();
            int dh = MathX.NextInt(bound < 0 ? NUMBER_OF_HASH_SLOTS + bound : bound);

            SimpleLog.i("Decreasing load for virtual node of " + node.toString() + ", delta h=" + dh);
            decreaseLoad(table, dh, vnode);
        }
    }

    @Override
    public void decreaseLoad(LookupTable table, int dh, Indexable node) {
        int hi = node.getHash();
        int hf = hi - dh;
        if (hf < 0) hf = NUMBER_OF_HASH_SLOTS + hf;

        if (dh == 0 || hf == LookupTable.getInstance().getTable().pre(node).getHash()) {
            SimpleLog.i("Invalid hash change, delta h=" + dh);
            return;
        }

        Indexable toNode = table.getTable().get(node.getIndex() + NUMBER_OF_REPLICAS);
        transfer(hf, hi, node, toNode);   // transfer(start, end, from, to). (start, end]

        node.setHash(hf);
        SimpleLog.i("Decreased load for virtual node " + hi + " to " + hf);
        SimpleLog.i("Updated node info: " + node.toString());
    }

    @Override
    public void increaseLoad(LookupTable table, int dh, Indexable node) {
        int hi = node.getHash();
        int hf = (hi + dh) % NUMBER_OF_HASH_SLOTS;

        if (dh == 0 || hf == LookupTable.getInstance().getTable().next(node).getHash()) {
            SimpleLog.i("Invalid hash change, delta h=" + dh);
            return;
        }

        Indexable fromNode = table.getTable().get(node.getIndex() + NUMBER_OF_REPLICAS);
        requestTransfer(hi, hf, fromNode, node); // requestTransfer(start, end, from, to). (start, end]

        node.setHash(hf);
        SimpleLog.i("Increased load for virtual node " + hi + " to " + hf);
        SimpleLog.i("Updated node info: " + node.toString());
    }

    @Override
    public void nodeJoin(LookupTable table, Indexable node) {
        SimpleLog.i("Adding virtual node [hash=" + node.getHash() + "] for " + ((VirtualNode)node).getPhysicalNodeId());

        Indexable successor = table.getTable().next(node);
        Indexable startNode = table.getTable().get(node.getIndex() - NUMBER_OF_REPLICAS);
        Indexable endNode = table.getTable().next(startNode);

        for (int i = 0; i < NUMBER_OF_REPLICAS; i++) {
            int hi = startNode.getHash();
            int hf = endNode.getHash();

            requestTransfer(hi, hf, successor, node); // requestTransfer(start, end, from, to). (start, end]

            startNode = endNode;
            endNode = table.getTable().next(startNode);
            successor = table.getTable().next(successor);
        }

        SimpleLog.i("Virtual node [hash=" + node.getHash() + "] added");
    }

    @Override
    public void nodeLeave(LookupTable table, Indexable node) {
        SimpleLog.i("Removing virtual node [hash=" + node.getHash() + "] from " + ((VirtualNode)node).getPhysicalNodeId());

        Indexable successor = table.getTable().get(node.getIndex());
        Indexable predecessor = table.getTable().pre(successor);
        Indexable startNode = table.getTable().get(node.getIndex() - NUMBER_OF_REPLICAS);
        Indexable endNode = table.getTable().next(startNode);

        for (int i = 0; i < NUMBER_OF_REPLICAS; i++) {
            int hi = startNode.getHash();
            int hf = endNode.getHash();

            requestReplication(hi, hf, predecessor, successor); // requestTransfer(start, end, from, to). (start, end]

            startNode = endNode;
            endNode = table.getTable().next(startNode);
            predecessor = successor;
            successor = table.getTable().next(successor);
        }

        SimpleLog.i("Virtual node [hash=" + node.getHash() + "] removed");
    }

    private void transfer(int hi, int hf, Indexable fromNode, Indexable toNode) {
        SimpleLog.i("Transfer hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
    }

    private void requestTransfer(int hi, int hf, Indexable fromNode, Indexable toNode) {
        SimpleLog.i("Request to transfer hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
    }

    private void requestReplication(int hi, int hf, Indexable fromNode, Indexable toNode) {
        SimpleLog.i("Copy hash (" + hi + ", "+ hf + "] from " + fromNode.toString() + " to " + toNode.toString());
    }
}
