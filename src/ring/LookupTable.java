package ring;

import commonmodels.*;
import filemanagement.DummyFile;
import filemanagement.FileBucket;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class LookupTable extends Transportable implements Serializable {

    private long epoch;

    private BinarySearchList table;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private transient RingLoadBalanceAlgorithm loadBalanceAlgorithm;

    private transient RingVNodeLoadBalanceAlgorithm vNodeLoadBalanceAlgorithm;

    private transient RingForwardLoadBalanceAlgorithm forwardLoadBalanceAlgorithm;

    private transient RingMembershipAlgorithm membershipAlgorithm;

    private transient RingReadWriteAlgorithm readWriteAlgorithm;

    private transient MembershipCallBack membershipCallBack;

    private transient ReadWriteCallBack readWriteCallBack;

    private transient Supplier deltaSupplier;

    private static volatile LookupTable instance = null;

    private LookupTable() {
        physicalNodeMap = new HashMap<>();
        table = new BinarySearchList();
        epoch = 0;

        membershipAlgorithm = new RingMembershipAlgorithm();
        loadBalanceAlgorithm = new RingLoadBalanceAlgorithm();
        vNodeLoadBalanceAlgorithm = new RingVNodeLoadBalanceAlgorithm();
        forwardLoadBalanceAlgorithm = new RingForwardLoadBalanceAlgorithm();
        readWriteAlgorithm = new RingReadWriteAlgorithm();
    }

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

    public static void deleteInstance() {
        instance = null;
    }

    public String createTable(Object o) {
        if (o instanceof LookupTable) {
            LookupTable remoteTable = (LookupTable)o;
            this.setTable(remoteTable.getTable());
            this.setEpoch(remoteTable.getEpoch());
            this.setPhysicalNodeMap(remoteTable.getPhysicalNodeMap());

            return "Table updated.";
        }
        else {
            return "Invalid table type.";
        }
    }

    public String updateTable(Object o) {
        if (o instanceof LookupTable) {
            LookupTable remoteTable = (LookupTable)o;

            if (this.table == null || remoteTable.getEpoch() > this.getEpoch()) {
                this.setTable(remoteTable.getTable());
                this.setEpoch(remoteTable.getEpoch());
                this.setPhysicalNodeMap(remoteTable.getPhysicalNodeMap());

                return "Table updated.";
            }
            else {
                return "Obsolete table. No need to update";
            }
        }
        else {
            return "Invalid table type.";
        }
    }

    public void initialize() {
        membershipAlgorithm.initialize(this);
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

    public Supplier getDeltaSupplier() {
        return deltaSupplier;
    }

    public void setDeltaSupplier(Supplier deltaSupplier) {
        this.deltaSupplier = deltaSupplier;
    }

    public void update() {
        epoch = System.currentTimeMillis();
    }

    public void addNode(Indexable node) {
        Indexable index = table.findIndex(node); // where the new node is inserted to
        table.add(index.getIndex(), node); // only add the node to table, not gossiping the change yet
        loadBalanceAlgorithm.nodeJoin(this, index);
    }

    public void removeNode(Indexable node) {
        Indexable index = table.findIndex(node); // where the new node is inserted to
        table.remove(index.getIndex()); // only remove from table, not gossiping the change yet
        loadBalanceAlgorithm.nodeLeave(this, index);
    }

    public void addNode(PhysicalNode node) {
        membershipAlgorithm.addPhysicalNode(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void addNode(PhysicalNode node, int[] buckets) {
        membershipAlgorithm.addPhysicalNode(this, node, buckets);
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

    public void increaseLoad(PhysicalNode node, int[] hashVal) {
        loadBalanceAlgorithm.increaseLoad(this, node, hashVal);
        update(); // commit the change, gossip to other nodes
    }

    public void decreaseLoad(PhysicalNode node) {
        loadBalanceAlgorithm.decreaseLoad(this, node);
        update(); // commit the change, gossip to other nodes
    }

    public void decreaseLoad(PhysicalNode node, int[] hashVal) {
        loadBalanceAlgorithm.decreaseLoad(this, node, hashVal);
        update(); // commit the change, gossip to other nodes
    }

    public void decreaseLoad(PhysicalNode node, int hash, long delta) {
        Indexable vnode = table.findNode(new VirtualNode(hash));
        forwardLoadBalanceAlgorithm.decreaseLoad(this, node, vnode, delta);
    }

    public void moveVNode(VirtualNode node, PhysicalNode from , PhysicalNode to) {
        vNodeLoadBalanceAlgorithm.moveVNode(this, node, from, to);
        update(); // commit the change, gossip to other nodes
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

    public int[] getSpareBuckets() {
        return membershipAlgorithm.generateSpareBuckets(this);
    }

    public int[] randomIncreaseRange(PhysicalNode node) {
        return loadBalanceAlgorithm.randomIncreaseRange(this, node);
    }

    public int[] randomDecreaseRange(PhysicalNode node) {
        return loadBalanceAlgorithm.randomDecreaseRange(this, node);
    }

    @Override
    public String toString() {
        return "Epoch: " + epoch + "\n" + table.toString();
    }
}
