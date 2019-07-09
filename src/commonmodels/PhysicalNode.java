package commonmodels;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static util.Config.STATUS_ACTIVE;

public class PhysicalNode implements Clusterable, Serializable {

    private String address;

    private int port;

    private String status;

    private List<Indexable> virtualNodes;

    private float weight; // for ceph only

    public PhysicalNode() {
        virtualNodes = new ArrayList<>();
        status = STATUS_ACTIVE;
    }

    public PhysicalNode(String address, int port) {
        this();
        this.address = address;
        this.port = port;
    }

    public PhysicalNode(String address) {
        this();
        String[] addressParams = address.split(":");
        this.address = addressParams[0];
        this.port = Integer.valueOf(addressParams[1]);
    }

    public String getId() {
        return "P" + getAddress() + ":" + getPort();
    }

    @Override
    public void setId(String id) {

    }


    @Override
    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    @Override
    public Clusterable[] getSubClusters() {
        return null;
    }

    @Override
    public void setSubClusters(Clusterable[] subClusters) {

    }

    /**
     * For Ceph scheme only
     *
     * @return all leave nodes, which are the physical nodes.
     */
    @Override
    public List<Clusterable> getLeaves() {
        List<Clusterable> leaves = new ArrayList<>();
        leaves.add(this);
        return leaves;
    }

    public String getAddress() {
        return address;
    }

    public String getFullAddress() {
        return address + ":" + port;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public void updateWeight() {
        // stub method, for ceph only
    }

    @Override
    public int getNumberOfSubClusters() {
        // stub method, for ceph only
        return 0;
    }

    public List<Indexable> getVirtualNodes() {
        return virtualNodes;
    }

    public void setVirtualNodes(List<Indexable> virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("PhysicalNode{" + "ip='").append(address).append('\'').append(", port=").append(port).append(", status='").append(status).append('\'').append(", virtualNodes=");

        for (Indexable indexable : virtualNodes) {
            result.append(indexable.getDisplayId()).append(" ");
        }
        result.append('}');
        return result.toString();
    }

    /**
     * For Ceph only
     * @param prefix
     * @param isTail
     */
    @Override
    public String toTreeString(String prefix, boolean isTail) {
        return prefix + (isTail ? "└── " : "├── ") + getId() + ": " + getStatus() + ", weight: " + weight + "\n";
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof PhysicalNode) == false) {
            return false;
        }
        PhysicalNode rhs = ((PhysicalNode) other);
        return new EqualsBuilder().append(getId(), rhs.getId()).isEquals();
    }
}
