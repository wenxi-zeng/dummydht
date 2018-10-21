package models;

import algorithms.replciaplacement.ReplicaPlacementAlgorithm;

import java.util.HashMap;
import java.util.List;

public class LookupTable {

    private long epoch;

    private BinarySearchList table;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private ReplicaPlacementAlgorithm algorithm;

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

    public void update() {

    }

    public void addNode(int index, Indexable node) {

    }

    public void remove(Indexable index) {

    }
}
