package ceph;

import commonmodels.Clusterable;

public class Cluster implements Clusterable {

    private String id;

    private float weight;

    private Clusterable[] subCluster;

    private String status;

    @Override
    public String getId() {
        return id;
    }

    @Override
    public float getWeight() {
        return weight;
    }

    @Override
    public Clusterable[] getSubClusters() {
        return subCluster;
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public void setWeight(float weight) {
        this.weight = weight;
    }

    @Override
    public void setStatus(String status) {
        this.status = status;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setSubCluster(Clusterable[] subCluster) {
        this.subCluster = subCluster;
    }
}
