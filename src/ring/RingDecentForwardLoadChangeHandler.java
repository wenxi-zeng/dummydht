package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import loadmanagement.LoadInfo;
import util.Config;

import java.util.List;

public class RingDecentForwardLoadChangeHandler extends RingForwardLoadChangeHandler {

    private final int maxLookForward;

    private final int numOfReplicas;

    public RingDecentForwardLoadChangeHandler(LookupTable table) {
        super(table);
        maxLookForward = Config.getInstance().getMaxLookForward();
        numOfReplicas = Config.getInstance().getNumberOfReplicas();
    }

    @Override
    protected boolean isEligibleToBalance(Indexable curr, List<LoadInfo> loadInfoList) {
        if (loadInfoList == null || loadInfoList.size() < 1) return false;

        PhysicalNode node = new PhysicalNode(loadInfoList.get(0).getNodeId());
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) return false;

        Indexable iterator = curr;
        for (int k = 0; k < maxLookForward; k++) {
            Indexable nonReplicaSuccessor = table.getTable().get(iterator.getIndex() + numOfReplicas);

            // if we already checked all of numOfReplicas-th successors
            // that means no nodes are available for load balancing.
            if (nonReplicaSuccessor.getIndex() == curr.getIndex()) {
                return false;
            }

            for (Indexable vnode : pnode.getVirtualNodes()) {
                if (vnode.getIndex() == nonReplicaSuccessor.getIndex())
                    return true;
            }

            iterator = nonReplicaSuccessor;
        }

        return false;
    }

    @Override
    public long computeTargetLoad(List<LoadInfo> loadInfoList, LoadInfo loadInfo, long lowerBound, long upperBound) {
        if (loadInfoList == null || loadInfoList.size() < 1) return lowerBound;
        else return loadInfo.getLoad() - (upperBound - loadInfoList.get(0).getLoad());
    }

}
