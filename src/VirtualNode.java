public class VirtualNode implements Comparable<VirtualNode> {

    private int hash;

    private int index;

    private String physicalNodeId;

    public VirtualNode() {
        this.index = -1;
    }

    public VirtualNode(int hash, String physicalNodeId) {
        this.hash = hash;
        this.physicalNodeId = physicalNodeId;
        this.index = -1;
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
    public int compareTo(VirtualNode o) {
        if (this.hash < o.getHash())
            return -1;
        else if (this.hash > o.getHash())
            return 1;
        else
            return 0;
    }
}
