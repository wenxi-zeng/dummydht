package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.List;

public class CephDecentLoadChangeHandler extends CephLoadChangeHandler {
    public CephDecentLoadChangeHandler(ClusterMap map) {
        super(map);
    }

    @Override
    public List<Request> generateRequestBasedOnLoad(List<LoadInfo> loadInfoList, LoadInfo loadInfo, long lowerBound, long upperBound) {
        if (loadInfoList == null || loadInfoList.size() < 1) return null;

        PhysicalNode node = new PhysicalNode(loadInfo.getNodeId());
        node = map.getPhysicalNodeMap().get(node.getId());
        Clusterable parent = map.findParentOf(node);

        float halfWeight = 0;
        for (int i = 0; i < parent.getSubClusters().length; i++) {
            Clusterable child = parent.getSubClusters()[i];
            if (child == null) continue;
            if (child instanceof PhysicalNode) {
                PhysicalNode pchild = (PhysicalNode) child;

                if (pchild.getId().equals(node.getId())) {
                    halfWeight = pchild.getWeight() / 2;
                }
            }
        }

        for (int i = 0; i < parent.getSubClusters().length; i++) {
            Clusterable child = parent.getSubClusters()[i];
            if (child == null) continue;
            if (child instanceof PhysicalNode) {
                PhysicalNode pchild = (PhysicalNode) child;

                if (pchild.getId().equals(node.getId())) {
                    pchild.setWeight(halfWeight);
                }
                else if(loadInfoList.get(0).getNodeId().equals(node.getId())) {
                    pchild.setWeight(pchild.getWeight() + halfWeight);
                }
            }
        }

        // no need to generate request for changed nodes,
        // since every node has to have a update request.
        // requests are generated in the optimize method.
        return null;
    }
}
