package ceph.strategies;

import ceph.ClusterMap;
import commonmodels.Clusterable;

public class InClusterStrategy implements WeightDistributeStrategy{
    @Override
    public void onNodeAddition(ClusterMap map, Clusterable parent, Clusterable child) {
        float redistributeWeight = parent.getWeight() / parent.getNumberOfSubClusters();
        for (int i = parent.getSubClusters().length - 1; i >= 0; i--) {
            if (parent.getSubClusters()[i] != null) {
                parent.getSubClusters()[i].setWeight(redistributeWeight);
            }
        }
    }

    @Override
    public void onWeightChanged(ClusterMap map, Clusterable clusterable, float deltaWeight) {
        Clusterable parent = map.findParentOf(clusterable);

        if (parent.getNumberOfSubClusters() < 2) {
            clusterable.setWeight(clusterable.getWeight() + deltaWeight);
            map.getRoot().updateWeight();
        }
        else  {
            float finalWeight = clusterable.getWeight() + deltaWeight;
            float redistributeWeight = deltaWeight / parent.getNumberOfSubClusters() - 1;
            for (int i = parent.getSubClusters().length - 1; i >= 0; i--) {
                Clusterable child = parent.getSubClusters()[i];
                if (child != null) {
                    child.setWeight(child.getWeight() - redistributeWeight);
                }
            }
            clusterable.setWeight(finalWeight);
        }
    }
}
