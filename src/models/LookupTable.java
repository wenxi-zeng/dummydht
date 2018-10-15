package models;

import algorithms.ReplicaPlacementAlgorithm;

import java.util.HashMap;
import java.util.List;

public class LookupTable {

    private long epoch;

    private BinarySearchList table;

    private HashMap<String, PhysicalNode> physicalNodeMap;

    private ReplicaPlacementAlgorithm algorithm;

    public LookupTable() {
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
}
