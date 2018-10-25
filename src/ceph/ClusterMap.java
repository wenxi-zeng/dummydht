package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import util.MathX;

import java.util.*;

import static util.Config.NUMBER_OF_PLACEMENT_GROUPS;

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
        root = new Cluster();

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

    public Clusterable findCluster(String id) {
        Queue<Clusterable> frontier = new LinkedList<>();
        frontier.add(root);

        while (!frontier.isEmpty()) {
            Clusterable cluster = frontier.poll();
            if (cluster.getId().equals(id)) return cluster;

            for (Clusterable child : cluster.getSubClusters()) {
                if (child != null)
                    frontier.add(child);
            }
        }

        return null;
    }

    public Clusterable rush(String pgid, int r) {
        Queue<Clusterable> frontier = new LinkedList<>();
        frontier.add(root);

        while (!frontier.isEmpty()) {
            Clusterable cluster = frontier.poll();

            for (int i = 0; i < cluster.getSubClusters().length; i++) {
                Clusterable subCluster = cluster.getSubClusters()[i];
                if (subCluster == null) continue;

                double rushHash = MathX.rushHash(pgid, r, subCluster.getId());
                double ratio = subtotalWeightRatio(i, cluster.getSubClusters());
                if (rushHash < ratio) {
                    if (subCluster instanceof PhysicalNode) {
                        return subCluster;
                    }
                    else {
                        frontier.add(subCluster);
                        break;
                    }
                }
            }
        }

        return null;
    }

    public String getPlacementGroupId(String name) {
        int pgid = MathX.positiveHash(name.hashCode()) % NUMBER_OF_PLACEMENT_GROUPS;
        return "PG" + pgid;
    }

    public String getPlacementGroupId(int pgid) {
        return "PG" + pgid;
    }

    private float subtotalWeightRatio(int index, Clusterable[] clusters) {
        float subtotal = 0;

        for (int i = index; i < clusters.length; i++) {
            subtotal += clusters[i].getWeight();
        }

        return subtotal == 0 ? 1 : clusters[index].getWeight() / subtotal;
    }

    public void update() {
        epoch = System.currentTimeMillis();
    }

    public void onNodeFailureOrRemoval(Clusterable failedNode) {
        loadBalanceAlgorithm.failureRecovery(this, failedNode);
    }

    public void onNodeAddition(Clusterable clusterable) {
        loadBalanceAlgorithm.loadBalancingForNewMember(this, clusterable);
    }

    public void loadBalancing(Clusterable clusterable) {
        loadBalanceAlgorithm.loadBalancing(this, clusterable);
        update();
    }

    public void addNode(String clusterId, PhysicalNode node) {
        membershipAlgorithm.addPhysicalNode(this, clusterId, node);
        update(); // commit the change, gossip to other nodes
    }

    public void removeNode(PhysicalNode node) {
        membershipAlgorithm.removePhysicalNode(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void changeWeight(PhysicalNode node, float deltaWeight) {
        loadBalanceAlgorithm.changeWeight(this, node, deltaWeight);
        update(); // commit the change, gossip to other nodes
    }

    public List<Clusterable> write(String filename) {
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
        return "Epoch: " + epoch + "\n" + root.toTreeString("", true);
    }
}
