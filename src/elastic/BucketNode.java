package elastic;

import commonmodels.Indexable;

import java.util.ArrayList;
import java.util.List;

public class BucketNode implements Indexable {

    private int hash;

    private List<String> physicalNodes;

    public BucketNode() {
        physicalNodes = new ArrayList<>();
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

    public List<String> getPhysicalNodes() {
        return physicalNodes;
    }

    public void setPhysicalNodes(List<String> physicalNodeId) {
        this.physicalNodes = physicalNodeId;
    }

    public int getIndex() {
        return hash;
    }

    public void setIndex(int index) {
        // stub method
    }

    @Override
    public int compareTo(Indexable o) {
        return Integer.compare(this.hash, o.getHash());
    }
}