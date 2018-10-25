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
    public String getDisplayId() {
        return String.valueOf(getHash());
    }

    @Override
    public int compareTo(Indexable o) {
        return Integer.compare(this.hash, o.getHash());
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "hash=" + hash +
                ", physicalNodes=" + physicalNodes +
                "}\n";
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BucketNode)
            return this.hashCode() == obj.hashCode();
        else
            return false;
    }
}