package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;
import util.Config;

import java.util.List;

public class RingDecentLoadChangeHandler extends RingLoadChangeHandler {
    public RingDecentLoadChangeHandler(LookupTable table) {
        super(table);
    }

    @Override
    protected boolean isEligibleToBalance(Indexable curr, List<LoadInfo> loadInfoList) {
        if (loadInfoList == null || loadInfoList.size() < 1) return false;

        PhysicalNode node = new PhysicalNode(loadInfoList.get(0).getNodeId());
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) return false;
        Indexable targetNode = table.getTable().get(curr.getIndex() + Config.getInstance().getNumberOfReplicas() - 1);

        for (Indexable vnode : pnode.getVirtualNodes()) {
            if (vnode.getIndex() == targetNode.getIndex())
                return true;
        }

        return false;
    }

    @Override
    public long computeTargetLoad(List<LoadInfo> loadInfoList, LoadInfo loadInfo, long lowerBound, long upperBound) {
        if (loadInfoList == null || loadInfoList.size() < 1) return lowerBound;
        else return loadInfo.getLoad() - (upperBound - loadInfoList.get(0).getLoad());
    }
}
