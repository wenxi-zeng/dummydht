package elastic;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.Config;

import java.io.Serializable;
import java.util.*;

public class LookupTable implements Serializable {

    private long epoch;

    private BucketNode[] table;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private transient ElasticLoadBalanceAlgorithm loadBalanceAlgorithm;

    private transient ElasticMembershipAlgorithm membershipAlgorithm;

    private transient ElasticReadWriteAlgorithm readWriteAlgorithm;

    private static volatile LookupTable instance = null;

    private LookupTable() {
        physicalNodeMap = new HashMap<>();
        epoch = System.currentTimeMillis();

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
        Config.NUMBER_OF_HASH_SLOTS *= 2;
        int i = table.length;
        table = Arrays.copyOf(table, Config.NUMBER_OF_HASH_SLOTS);

        for (; i < table.length; i++) {
            table[i] = new BucketNode(i);
        }
    }

    protected void shrinkTable() {
        Config.NUMBER_OF_HASH_SLOTS /= 2;
        table = Arrays.copyOf(table, Config.NUMBER_OF_HASH_SLOTS);
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

    public void copyBucket(BucketNode node, PhysicalNode to) {
        loadBalanceAlgorithm.copyBucket(this, node, to);
    }

    public Indexable write(String filename) {
        return readWriteAlgorithm.write(this, filename);
    }

    public Indexable read(String filename) {
        return readWriteAlgorithm.read(this, filename);
    }

    public void expand() {
        loadBalanceAlgorithm.onTableExpand(this);
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
}
