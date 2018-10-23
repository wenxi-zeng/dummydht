package commonmodels;

import java.util.ArrayList;
import java.util.List;

public class PhysicalNode {

    private String address;

    private int port;

    private String status;

    private List<Indexable> virtualNodes;

    public final static String STATUS_ACTIVE = "active";

    public final static String STATUS_INACTIVE = "inactive";

    public PhysicalNode() {
        virtualNodes = new ArrayList<>();
    }

    public String getId() {
        return "P" + getAddress() + ":" + getPort();
    }

    public String getAddress() {
        return address;
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

    public List<Indexable> getVirtualNodes() {
        return virtualNodes;
    }

    public void setVirtualNodes(List<Indexable> virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    @Override
    public String toString() {
        return address + ":" + port;
    }
}
