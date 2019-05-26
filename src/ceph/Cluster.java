package ceph;

import commonmodels.Clusterable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Cluster implements Clusterable, Serializable {

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
    public void setSubClusters(Clusterable[] subClusters) {
        this.subCluster = subClusters;
    }

    @Override
    public List<Clusterable> getLeaves() {
        List<Clusterable> results = new ArrayList<>();

        for (Clusterable cluster : subCluster) {
            if (cluster != null) {
                results.addAll(cluster.getLeaves());
            }
        }

        return results;
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

    @Override
    public void updateWeight() {
        float newWeight = 0;

        for (Clusterable subCluster : getSubClusters()) {
            if (subCluster != null) {
                subCluster.updateWeight();
                newWeight += subCluster.getWeight();
            }
        }

        this.weight = newWeight;
    }

    @Override
    public int getNumberOfSubClusters() {
        int count = 0;

        for (Clusterable subCluster : getSubClusters()) {
            if (subCluster != null) {
                count ++;
            }
        }

        return count;
    }

    @Override
    public String toTreeString(String prefix, boolean isTail) {
        StringBuilder result = new StringBuilder();
        result.append(prefix).append(isTail ? "└── " : "├── ").append(getId()).append(", weight: ").append(weight).append('\n');
        for (int i = 0; i < subCluster.length - 1; i++) {
            if (subCluster[i] == null) continue;
            result.append(subCluster[i].toTreeString(prefix + (isTail ? "    " : "│   "), false));
        }
        if (subCluster.length > 0) {
            result.append(subCluster[subCluster.length - 1]
                    .toTreeString(prefix + (isTail ?"    " : "│   "), true));
        }

        return result.toString();
    }

    public void setId(String id) {
        this.id = id;
    }
}
