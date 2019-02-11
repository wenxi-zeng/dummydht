package ceph;

import commands.CephCommand;
import commonmodels.Clusterable;
import commonmodels.LoadChangeHandler;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CephLoadChangeHandler implements LoadChangeHandler {

    private final ClusterMap map;

    public CephLoadChangeHandler(ClusterMap map) {
        this.map = map;
    }

    @Override
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        PhysicalNode node = new PhysicalNode(loadInfo.getNodeId());
        node = map.getPhysicalNodeMap().get(node.getId());
        Clusterable parent = map.findParentOf(node);

        Map<String, LoadInfo> map = globalLoad.stream().collect(
                Collectors.toMap(LoadInfo::getNodeId, info -> info));

        long totalLoad = 0;
        long totalCapacity = 0;
        for (int i = parent.getSubClusters().length - 1; i >= 0; i--) {
            Clusterable child = parent.getSubClusters()[i];
            if (child instanceof PhysicalNode) {
                LoadInfo info = map.get(((PhysicalNode) child).getFullAddress());
                totalLoad += info.getFileLoad();
                totalCapacity += child.getWeight();
            }
        }

        if (totalLoad < totalCapacity) {
            return new Request().withHeader(CephCommand.CHANGEWEIGHT.name())
                    .withAttachment(loadInfo.getNodeId() + " " + ((loadInfo.getFileLoad() - upperBound + (upperBound - lowerBound) / 2) * -1));
        }
        else {
            return null;
        }
    }
}
