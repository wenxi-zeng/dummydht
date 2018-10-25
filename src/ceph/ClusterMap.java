package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;

import java.util.HashMap;
import java.util.Map;

public class ClusterMap {

    private Clusterable root;

    private long epoch;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private CephLoadBalanceAlgorithm loadBalanceAlgorithm;

    private CephMembershipAlgorithm membershipAlgorithm;

    private CephReadWriteAlgorithm readWriteAlgorithm;

    private static volatile ClusterMap instance = null;

    private ClusterMap() {
        initialize();
    }

    public static ClusterMap getInstance() {
        if (instance == null) {
            synchronized(ClusterMap.class) {
                if (instance == null) {
                    instance = new ClusterMap();
                }
            }
        }

        return instance;
    }

    public void initialize() {
        physicalNodeMap = new HashMap<>();
        epoch = System.currentTimeMillis();

        membershipAlgorithm = new CephMembershipAlgorithm();
        loadBalanceAlgorithm = new CephLoadBalanceAlgorithm();
        readWriteAlgorithm = new CephReadWriteAlgorithm();

        membershipAlgorithm.initialize(this);
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public Clusterable getRoot() {
        return root;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodeMap() {
        return physicalNodeMap;
    }

    public void setPhysicalNodeMap(HashMap<String, PhysicalNode> physicalNodeMap) {
        this.physicalNodeMap = physicalNodeMap;
    }

    public void update() {
        epoch = System.currentTimeMillis();
    }

    public void addNode(PhysicalNode node) {
        membershipAlgorithm.addPhysicalNode(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void removeNode(PhysicalNode node) {
        membershipAlgorithm.removePhysicalNode(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void increaseWeight(PhysicalNode node) {
        loadBalanceAlgorithm.increaseLoad(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void decreaseWeight(PhysicalNode node) {
        loadBalanceAlgorithm.decreaseLoad(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public Clusterable write(String filename) {
        return readWriteAlgorithm.write(this, filename);
    }

    public Clusterable read(String filename) {
        return readWriteAlgorithm.read(this, filename);
    }

    public String listPhysicalNodes() {
        StringBuilder result = new StringBuilder();

        for(Map.Entry entry : physicalNodeMap.entrySet()) {
            result.append(entry.getValue().toString()).append('\n');
        }

        return result.toString();
    }

    @Override
    public String toString() {
        return "Epoch: " + epoch + "\n" + Arrays.toString(table);
    }
}
