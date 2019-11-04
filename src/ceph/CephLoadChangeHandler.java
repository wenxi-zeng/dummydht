package ceph;

import commands.CephCommand;
import commonmodels.Clusterable;
import commonmodels.LoadChangeHandler;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.ArrayList;
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

        Map<String, LoadInfo> loadInfoMap = globalLoad.stream().collect(
                Collectors.toMap(LoadInfo::getNodeId, info -> info));
        float halfWeight = 0;
        int numberOfLightNodes = 0;
        for (int i = 0; i < parent.getSubClusters().length; i++) {
            Clusterable child = parent.getSubClusters()[i];
            if (child == null) continue;
            if (child instanceof PhysicalNode) {
                PhysicalNode pchild = (PhysicalNode) child;
                LoadInfo info = loadInfoMap.get(pchild.getFullAddress());
                if (info == null) continue;

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
                LoadInfo info = loadInfoMap.get(pchild.getFullAddress());
                if (info == null) continue;

                if (pchild.getId().equals(node.getId())) {
                    pchild.setWeight(halfWeight);
                }
                else if(info.getLoad() < lowerBound) {
                    pchild.setWeight(pchild.getWeight() + distributeWeight);
                }
            }
        }

        map.update();
        List<Request> requests = new ArrayList<>();
        requests.add(new Request()
                .withHeader(CephCommand.UPDATEMAP.name())
                .withLargeAttachment(map));

        return requests;
    }

    @Override
    public void optimize(List<Request> requests) {

    }

    @Override
    public long computeTargetLoad(List<LoadInfo> loadInfoList, LoadInfo loadInfo, long lowerBound, long upperBound) {
        // stub
        return 0;
    }
}
