package ceph;

import commands.CephCommand;
import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;

import java.util.ArrayList;
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

        List<Request> requests = new ArrayList<>();
        for (int i = 0; i < parent.getSubClusters().length; i++) {
            Clusterable child = parent.getSubClusters()[i];
            if (child == null) continue;
            if (child instanceof PhysicalNode) {
                PhysicalNode pchild = (PhysicalNode) child;
                if (pchild.getId().equals(node.getId())) {
                    Request r = new Request().withHeader(CephCommand.CHANGEWEIGHT.name())
                            .withReceiver(pchild.getFullAddress())
                            .withAttachments(pchild.getFullAddress(), pchild.getWeight() * 1.5);
                    requests.add(r);
                    break;
                }
            }
        }

        return requests;
    }
}
