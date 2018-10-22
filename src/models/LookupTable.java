package models;

import algorithms.loadbalance.LoadBalanceAlgorithm;
import algorithms.loadbalance.RingLoadBalanceAlgorithm;
import algorithms.membership.MembershipAlgorithm;
import algorithms.membership.RingMembershipAlgorithm;
import algorithms.readwrite.ReadWriteAlgorithm;
import algorithms.readwrite.RingReadWriteAlgorithm;
import algorithms.replciaplacement.ReplicaPlacementAlgorithm;
import algorithms.replciaplacement.RingReplicaAlgorithm;

import java.util.HashMap;
import java.util.List;

import static util.Config.CONFIG_RING;

public class LookupTable {

    private long epoch;

    private BinarySearchList table;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private ReplicaPlacementAlgorithm algorithm;

    private LoadBalanceAlgorithm loadBalanceAlgorithm;

    private MembershipAlgorithm membershipAlgorithm;

    private ReadWriteAlgorithm readWriteAlgorithm;

    private static volatile LookupTable instance = null;

    private LookupTable() {}

    public static LookupTable getInstance() {
        if (instance == null) {
            synchronized(LookupTable.class) {
                if (instance == null) {
                    instance = new LookupTable();
                }
            }
        }

        return instance;
    }

    public void initialize(String config) {
        physicalNodeMap = new HashMap<>();
        table = new BinarySearchList();
        epoch = System.currentTimeMillis();

        if (config.equals(CONFIG_RING)) {
            membershipAlgorithm = new RingMembershipAlgorithm();
            loadBalanceAlgorithm = new RingLoadBalanceAlgorithm();
            readWriteAlgorithm = new RingReadWriteAlgorithm();
            algorithm = new RingReplicaAlgorithm();
        }

        membershipAlgorithm.initialize(this);
    }

    public List<PhysicalNode> getReplicas(int hash) {
        return algorithm.getReplicas(this, hash);
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public BinarySearchList getTable() {
        return table;
    }

    public void setTable(BinarySearchList table) {
        this.table = table;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodeMap() {
        return physicalNodeMap;
    }

    public void setPhysicalNodeMap(HashMap<String, PhysicalNode> physicalNodeMap) {
        this.physicalNodeMap = physicalNodeMap;
    }

    public ReplicaPlacementAlgorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(ReplicaPlacementAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    public LoadBalanceAlgorithm getLoadBalanceAlgorithm() {
        return loadBalanceAlgorithm;
    }

    public void setLoadBalanceAlgorithm(LoadBalanceAlgorithm loadBalanceAlgorithm) {
        this.loadBalanceAlgorithm = loadBalanceAlgorithm;
    }

    public MembershipAlgorithm getMembershipAlgorithm() {
        return membershipAlgorithm;
    }

    public void setMembershipAlgorithm(MembershipAlgorithm membershipAlgorithm) {
        this.membershipAlgorithm = membershipAlgorithm;
    }

    public ReadWriteAlgorithm getReadWriteAlgorithm() {
        return readWriteAlgorithm;
    }

    public void setReadWriteAlgorithm(ReadWriteAlgorithm readWriteAlgorithm) {
        this.readWriteAlgorithm = readWriteAlgorithm;
    }

    public void update() {
        epoch = System.currentTimeMillis();
    }

    public void addNode(Indexable node) {
        Indexable index = table.find(node); // where the new node is inserted to
        table.add(index.getIndex(), node); // only add the node to table, not gossiping the change yet
        loadBalanceAlgorithm.nodeJoin(this, index);
    }

    public void removeNode(Indexable node) {
        Indexable index = table.find(node); // where the new node is inserted to
        table.remove(index.getIndex()); // only remove from table, not gossiping the change yet
        loadBalanceAlgorithm.nodeLeave(this, index);
    }

    public void addNode(PhysicalNode node) {
        membershipAlgorithm.addPhysicalNode(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void removeNode(PhysicalNode node) {
        membershipAlgorithm.removePhysicalNode(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void increaseLoad(PhysicalNode node) {
        loadBalanceAlgorithm.increaseLoad(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void decreaseLoad(PhysicalNode node) {
        loadBalanceAlgorithm.decreaseLoad(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public Indexable write(String filename) {
        return readWriteAlgorithm.write(this, filename);
    }

    public Indexable read(String filename) {
        return readWriteAlgorithm.read(this, filename);
    }
}
