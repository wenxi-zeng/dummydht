package ceph.strategies;

import ceph.ClusterMap;
import commonmodels.Clusterable;

import static util.Config.INITIAL_WEIGHT;

public class CrossClustersStrategy implements WeightDistributeStrategy{

    @Override
    public void onNodeAddition(ClusterMap map, Clusterable parent, Clusterable child) {
        child.setWeight(INITIAL_WEIGHT);
        map.getRoot().updateWeight();
    }

    @Override
    public void onWeightChanged(ClusterMap map, Clusterable clusterable, float deltaWeight) {
        clusterable.setWeight(deltaWeight);
        map.getRoot().updateWeight();
    }

}
