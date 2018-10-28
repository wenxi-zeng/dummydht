package ceph.strategies;

import ceph.ClusterMap;
import commonmodels.Clusterable;

public interface WeightDistributeStrategy {

    void onNodeAddition(ClusterMap map, Clusterable parent, Clusterable child);

    void onWeightChanged(ClusterMap map, Clusterable clusterable, float deltaWeight);

}
