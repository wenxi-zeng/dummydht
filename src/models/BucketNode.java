package models;

import java.util.List;

public class BucketNode implements Indexable {

    private int hash;

    private int index;

    private List<String> physicalNodes;

    public BucketNode() {
        this.index = -1;
    }

    public BucketNode(int hash) {
        this();
        this.hash = hash;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public List<String> getpPhysicalNodes() {
        return physicalNodes;
    }

    public void setPhysicalNodes(List<String> physicalNodeId) {
        this.physicalNodes = physicalNodeId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public int compareTo(Indexable o) {
        return Integer.compare(this.hash, o.getHash());
    }
}