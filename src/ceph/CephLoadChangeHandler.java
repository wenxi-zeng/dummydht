package ceph;

import commands.CommonCommand;
import commonmodels.Clusterable;
import commonmodels.LoadChangeHandler;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CephLoadChangeHandler implements LoadChangeHandler {

    protected final ClusterMap map;

    public CephLoadChangeHandler(ClusterMap map) {
        this.map = map;
    }

    @Override
    public List<Request> generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        PhysicalNode node = new PhysicalNode(loadInfo.getNodeId());
        node = map.getPhysicalNodeMap().get(node.getId());
        Clusterable parent = map.findParentOf(node);

        Map<String, LoadInfo> map = globalLoad.stream().collect(
                Collectors.toMap(LoadInfo::getNodeId, info -> info));
        float halfWeight = 0;
        int numberOfLightNodes = 0;
        for (int i = 0; i < parent.getSubClusters().length; i++) {
            Clusterable child = parent.getSubClusters()[i];
            if (child == null) continue;
            if (child instanceof PhysicalNode) {
                PhysicalNode pchild = (PhysicalNode) child;
                LoadInfo info = map.get(pchild.getFullAddress());

                if (pchild.getId().equals(node.getId())) {
                    halfWeight = pchild.getWeight() / 2;
                }
                else if(info.getLoad() < lowerBound) {
                    numberOfLightNodes++;
                }
            }
        }

        if (numberOfLightNodes < 1) return null;

        float distributeWeight = halfWeight / numberOfLightNodes;
        for (int i = 0; i < parent.getSubClusters().length; i++) {
            Clusterable child = parent.getSubClusters()[i];
            if (child == null) continue;
            if (child instanceof PhysicalNode) {
                PhysicalNode pchild = (PhysicalNode) child;
                LoadInfo info = map.get(pchild.getFullAddress());

                if (pchild.getId().equals(node.getId())) {
                    pchild.setWeight(halfWeight);
                }
                else if(info.getLoad() < lowerBound) {
                    pchild.setWeight(pchild.getWeight() + distributeWeight);
                }
            }
        }

        // no need to generate request for changed nodes,
        // since every node has to have a update request.
        // requests are generated in the optimize method.
        return null;
    }

    @Override
    public void optimize(List<Request> requests) {
        requests.add(new Request().withHeader(CommonCommand.PROPAGATE.name()));
    }

    @Override
    public long computeTargetLoad(List<LoadInfo> loadInfoList, LoadInfo loadInfo, long lowerBound, long upperBound) {
        // stub
        return 0;
    }
}
