package algorithms.loadbalance;

import models.Indexable;
import models.LookupTable;

import static util.Config.NUMBER_OF_REPLICAS;

public class RingLoadBalanceAlgorithm implements LoadBalanceAlgorithm {
    @Override
    public void decreaseLoad(int dh, Indexable node) {
        int hi = node.getHash();
        int hf = hi - dh;

        if (!validMove(hf, node)) return;

        Indexable toNode = LookupTable.getInstance().getTable().get(node.getIndex() + NUMBER_OF_REPLICAS);
        transfer(hf, hi, node, toNode);   // transfer(start, end, from, to). (start, end]

        node.setHash(hf);
        LookupTable.getInstance().update(); // commit the change, gossip to other nodes
    }

    @Override
    public void increaseLoad(int dh, Indexable node) {
        int hi = node.getHash();
        int hf = hi + dh;

        if (!validMove(hf, node)) return;

        Indexable fromNode = LookupTable.getInstance().getTable().get(node.getIndex() + NUMBER_OF_REPLICAS);
        requestTransfer(hi, hf, fromNode, node); // requestTransfer(start, end, from, to). (start, end]

        node.setHash(hf);
        LookupTable.getInstance().update(); // commit the change, gossip to other nodes
    }

    @Override
    public void nodeJoin(Indexable node) {
        LookupTable table = LookupTable.getInstance();
        Indexable index = table.getTable().find(node); // where the new node is inserted to
        table.addNode(index.getIndex(), node); // only add the node to table, not gossiping the change yet

        Indexable successor = table.getTable().next(node);
        Indexable startNode = table.getTable().get(index.getIndex() - NUMBER_OF_REPLICAS);
        Indexable endNode = table.getTable().next(startNode);

        for (int i = 0; i < NUMBER_OF_REPLICAS; i++) {
            int hi = startNode.getHash();
            int hf = endNode.getHash();

            requestTransfer(hi, hf, successor, node); // requestTransfer(start, end, from, to). (start, end]

            startNode = endNode;
            endNode = table.getTable().next(startNode);
            successor = table.getTable().next(successor);
        }

        table.update(); // commit the change, gossip to other nodes
    }

    @Override
    public void nodeLeave(Indexable node) {
        LookupTable table = LookupTable.getInstance();
        Indexable index = table.getTable().find(node); // where the new node is inserted to
        table.remove(index); // only remove from table, not gossiping the change yet

        Indexable successor = table.getTable().get(index.getIndex());
        Indexable predecessor = table.getTable().pre(successor);
        Indexable startNode = table.getTable().get(index.getIndex() - NUMBER_OF_REPLICAS);
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

        table.update(); // commit the change, gossip to other nodes
    }

    private boolean validMove(int hf, Indexable node) {
        if (hf < node.getHash()) {
            // load decreased, check if hf is greater than predecessor
            return hf > LookupTable.getInstance().getTable().pre(node).getHash();
        }
        else {
            // load increased, check if hf is smaller than sucessor
            return hf < LookupTable.getInstance().getTable().next(node).getHash();
        }
    }

    private void transfer(int hi, int hf, Indexable fromNode, Indexable toNode) {

    }

    private void requestTransfer(int hi, int hf, Indexable fromNode, Indexable toNode) {

    }

    private void requestReplication(int hi, int hf, Indexable fromNode, Indexable toNode) {

    }
}
