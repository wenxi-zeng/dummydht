package elastic;

import commonmodels.*;
import filemanagement.DummyFile;
import filemanagement.FileBucket;
import util.Config;

import java.io.Serializable;
import java.util.*;

public class LookupTable extends Transportable implements Serializable {

    private long epoch;

    private BucketNode[] table;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private transient ElasticLoadBalanceAlgorithm loadBalanceAlgorithm;

    private transient ElasticMembershipAlgorithm membershipAlgorithm;

    private transient ElasticReadWriteAlgorithm readWriteAlgorithm;

    private transient LoadBalancingCallBack loadBalancingCallBack;

    private transient MembershipCallBack membershipCallBack;

    private transient ReadWriteCallBack readWriteCallBack;

    private static volatile LookupTable instance = null;

    private LookupTable() {
        physicalNodeMap = new HashMap<>();
        epoch = 0;

        membershipAlgorithm = new ElasticMembershipAlgorithm();
        loadBalanceAlgorithm = new ElasticLoadBalanceAlgorithm();
        readWriteAlgorithm = new ElasticReadWriteAlgorithm();
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

    public void initialize() {
        membershipAlgorithm.initialize(this);
    }

    public void createTable(int size) {
        table = new BucketNode[size];

        for (int i = 0; i < size; i++) {
            table[i] = new BucketNode(i);
        }
    }

    protected void expandTable() {
        int numberOfHashSlots = Config.getInstance().getNumberOfHashSlots();
        numberOfHashSlots *= 2;
        int i = table.length;
        table = Arrays.copyOf(table, numberOfHashSlots);

        for (; i < table.length; i++) {
            table[i] = new BucketNode(i);
        }
    }

    protected void shrinkTable() {
        int numberOfHashSlots = Config.getInstance().getNumberOfHashSlots();

        numberOfHashSlots /= 2;
        table = Arrays.copyOf(table, numberOfHashSlots);
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public BucketNode[] getTable() {
        return table;
    }

    public void setTable(BucketNode[] table) {
        this.table = table;
    }

    public HashMap<String, PhysicalNode> getPhysicalNodeMap() {
        return physicalNodeMap;
    }

    public void setPhysicalNodeMap(HashMap<String, PhysicalNode> physicalNodeMap) {
        this.physicalNodeMap = physicalNodeMap;
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

    public List<PhysicalNode> getOrderedPhysicalNodeList() {
        List<PhysicalNode> physicalNodes = new ArrayList<>(physicalNodeMap.values());
        physicalNodes.sort(Comparator.comparingInt(o -> o.getVirtualNodes().size()));

        return physicalNodes;
    }

    public void update() {
        epoch = System.currentTimeMillis();
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

    public void moveBucket(BucketNode node, PhysicalNode from , PhysicalNode to) {
        loadBalanceAlgorithm.moveBucket(this, node, from, to);
        update(); // commit the change, gossip to other nodes
    }

    public void moveBuckets(int[] buckets, PhysicalNode from , PhysicalNode to) {
        List<BucketNode> nodes = new ArrayList<>();
        for (int bucket : buckets)
            nodes.add(new BucketNode(bucket));
        moveBuckets(nodes, from, to);
    }

    public void moveBuckets(List<BucketNode> nodes, PhysicalNode from , PhysicalNode to) {
        loadBalanceAlgorithm.moveBuckets(this, nodes, from, to);
        update(); // commit the change, gossip to other nodes
    }

    public void copyBucket(BucketNode node, PhysicalNode to) {
        loadBalanceAlgorithm.copyBucket(this, node, to);
    }

    public void transferBucket(BucketNode node, PhysicalNode to) {
        loadBalanceAlgorithm.transferBucket(this, node, to);
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

    public void expand(int size) {
        loadBalanceAlgorithm.onTableExpand(this, size);
    }

    public void shrink() {
        loadBalanceAlgorithm.onTableShrink(this);
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

    @Override
    public String toString() {
        return "Epoch: " + epoch + "\n" + Arrays.toString(table);
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

            if (this.getTable() == null || remoteTable.getEpoch() > this.getEpoch()) {
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
}
