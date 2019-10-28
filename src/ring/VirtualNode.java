package ring;

import commonmodels.Indexable;
import commonmodels.Transportable;

public class VirtualNode extends Transportable implements Indexable {

    private int hash;

    private int index;

    private String physicalNodeId;

    public VirtualNode() {
        this.index = -1;
    }

    public VirtualNode(int hash) {
        this();
        this.hash = hash;
    }

    public VirtualNode(int hash, String physicalNodeId) {
        this(hash);
        this.physicalNodeId = physicalNodeId;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public String getPhysicalNodeId() {
        return physicalNodeId;
    }

    public void setPhysicalNodeId(String physicalNodeId) {
        this.physicalNodeId = physicalNodeId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String getDisplayId() {
        return String.valueOf(getHash());
    }

    public String getNodeId() {
        return physicalNodeId == null ? null : physicalNodeId.substring(1);
    }

    @Override
    public int compareTo(Indexable o) {
        return Integer.compare(this.hash, o.getHash());
    }

    @Override
    public String toString() {
        return "VirtualNode{" +
                "hash=" + hash +
                ", index=" + index +
                ", physicalNodeId='" + physicalNodeId + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VirtualNode)
            return this.hashCode() == obj.hashCode();
        else
            return false;
    }
}
