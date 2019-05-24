package ceph;

import ceph.strategies.WeightDistributeStrategy;
import commonmodels.*;
import filemanagement.DummyFile;
import filemanagement.FileBucket;
import util.Config;
import util.MathX;

import java.io.Serializable;
import java.util.*;

public class ClusterMap implements Serializable {

    private Clusterable root;

    private long epoch;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private transient CephLoadBalanceAlgorithm loadBalanceAlgorithm;

    private transient CephMembershipAlgorithm membershipAlgorithm;

    private transient CephReadWriteAlgorithm readWriteAlgorithm;

    private transient WeightDistributeStrategy weightDistributeStrategy;

    private transient LoadBalancingCallBack loadBalancingCallBack;

    private transient MembershipCallBack membershipCallBack;

    private transient ReadWriteCallBack readWriteCallBack;

    private transient PhysicalNode self;

    private static volatile ClusterMap instance = null;

    public static final String UPDATE_STATUS_DONE = "Map updated";

    public static final String UPDATE_STATUS_CANCEL = "Obsolete table. No need to update";

    public static final String UPDATE_STATUS_FAILED = "Invalid map type";

    private ClusterMap() {
        physicalNodeMap = new HashMap<>();
        epoch = System.currentTimeMillis();
        root = new Cluster();

        membershipAlgorithm = new CephMembershipAlgorithm();
        loadBalanceAlgorithm = new CephLoadBalanceAlgorithm();
        readWriteAlgorithm = new CephReadWriteAlgorithm();
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

    public static void deleteInstance() {
        instance = null;
    }

    public void initialize(String selfAddress) {
        if (selfAddress != null) {
            this.self = new PhysicalNode(selfAddress);
        }

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

    public void setRoot(Clusterable root) {
        this.root = root;
    }

    public WeightDistributeStrategy getWeightDistributeStrategy() {
        return weightDistributeStrategy;
    }

    public void setWeightDistributeStrategy(WeightDistributeStrategy weightDistributeStrategy) {
        this.weightDistributeStrategy = weightDistributeStrategy;
    }

    public LoadBalancingCallBack getLoadBalancingCallBack() {
        return loadBalancingCallBack;
    }

    public void setLoadBalancingCallBack(LoadBalancingCallBack loadBalancingCallBack) {
        this.loadBalancingCallBack = loadBalancingCallBack;
    }

    public MembershipCallBack getMembershipCallBack() {
        return membershipCallBack;
    }

    public void setMembershipCallBack(MembershipCallBack membershipCallBack) {
        this.membershipCallBack = membershipCallBack;
    }

    public ReadWriteCallBack getReadWriteCallBack() {
        return readWriteCallBack;
    }

    public void setReadWriteCallBack(ReadWriteCallBack readWriteCallBack) {
        this.readWriteCallBack = readWriteCallBack;
    }

    public Clusterable findCluster(String id) {
        Queue<Clusterable> frontier = new LinkedList<>();
        frontier.add(root);

        while (!frontier.isEmpty()) {
            Clusterable cluster = frontier.poll();
            if (cluster.getId().equals(id)) return cluster;
            if (cluster.getSubClusters() == null) continue;

            for (Clusterable child : cluster.getSubClusters()) {
                if (child != null)
                    frontier.add(child);
            }
        }

        return null;
    }

    public Clusterable findParentOf(Clusterable clusterable) {
        Stack<Clusterable> frontier = new Stack<>();
        frontier.push(root);

        while (!frontier.isEmpty()) {
            Clusterable cluster = frontier.pop();
            if (cluster.getSubClusters() == null) continue;

            for (Clusterable child : cluster.getSubClusters()) {
                if (child != null) {
                    if (child.getId().equals(clusterable.getId())) return cluster;
                    else frontier.push(child);
                }
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
                        // SimpleLog.i(pgid + ", r=" + r + ", rush: " + rushHash + ", ratio: " + ratio + ", node: " + subCluster.getId());
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
        int pgid = MathX.positiveHash(name.hashCode()) % Config.getInstance().getNumberOfPlacementGroups();
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

    public void loadBalancing(Clusterable clusterable) {
        loadBalanceAlgorithm.loadBalancing(this, clusterable);
        update();
    }

    public void scheduleLoadBalancing() {
        loadBalancing(root);
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

    public void propagateTableChanges() {
        loadBalanceAlgorithm.propagateTableChanges(this);
        update();
    }

    public List<PhysicalNode> lookup(String filename) {
        return readWriteAlgorithm.lookup(this, filename);
    }

    public FileBucket write(DummyFile file, boolean replicate) {
        if (replicate)
            return readWriteAlgorithm.writeAndReplicate(this, file);
        else
            return readWriteAlgorithm.writeOnly(this, file);
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

    public String updateTable(Object o) {
        if (o instanceof ClusterMap) {
            ClusterMap remoteMap = (ClusterMap)o;

            if (this.getRoot() == null || remoteMap.getEpoch() > this.getEpoch()) {
                this.setRoot(remoteMap.getRoot());
                this.setEpoch(remoteMap.getEpoch());
                this.setPhysicalNodeMap(remoteMap.getPhysicalNodeMap());

                return UPDATE_STATUS_DONE;
            }
            else {
                return UPDATE_STATUS_CANCEL;
            }
        }
        else {
            return UPDATE_STATUS_FAILED;
        }
    }

    public List<Clusterable> getSpareEdges() {
        List<Clusterable> spareEdges = new ArrayList<>();
        Queue<Clusterable> frontier = new LinkedList<>();
        frontier.add(root);

        while (!frontier.isEmpty()) {
            Clusterable cluster = frontier.poll();

            for (int i = 0; i < cluster.getSubClusters().length; i++) {
                Clusterable subCluster = cluster.getSubClusters()[i];
                if (subCluster == null) {
                    spareEdges.add(cluster);
                    break;
                }
                else if (!(subCluster instanceof PhysicalNode)){
                    frontier.add(subCluster);
                }
            }
        }

        return spareEdges;
    }

    public PhysicalNode getSelf() {
        return physicalNodeMap.get(self.getId());
    }
}
