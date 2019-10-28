package ring;

import commands.RingCommand;
import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import loadmanagement.LoadInfo;
import util.Config;

import java.util.ArrayList;
import java.util.List;

public class RingForwardLoadChangeHandler extends RingLoadChangeHandler {

    private int maxLookForward;

    private int numOfReplicas;

    public RingForwardLoadChangeHandler(LookupTable table) {
        super(table);
        maxLookForward = Config.getInstance().getMaxLookForward();
        numOfReplicas = Config.getInstance().getNumberOfReplicas();
    }

    @Override
    public List<Request> generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        PhysicalNode node = new PhysicalNode(loadInfo.getNodeId());
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        List<Solution> bestSolutions = null;
        long target = computeTargetLoad(globalLoad, loadInfo, lowerBound, upperBound);

        if (pnode == null || pnode.getVirtualNodes() == null) return null;
        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            if (!isEligibleToBalance(vnode, globalLoad)) continue;

            List<Solution> solutions = evaluate(globalLoad, loadInfo, vnode, target, upperBound);

            if (solutions == null) continue;
            if (bestSolutions == null || solutions.size() < bestSolutions.size())
                bestSolutions = solutions;
        }

        if (bestSolutions == null) return null;

        List<Request> requests = new ArrayList<>();
        for (Solution solution : bestSolutions) {
            requests.add(generateRequestBasedOnSolution(pnode, solution));
        }

        return requests;
    }

    private List<Solution> evaluate(List<LoadInfo> globalLoad, LoadInfo loadInfo, Indexable current, long target, long upperBound) {
        List<Indexable> sequence = getLookForwardSequence(globalLoad, current, upperBound);
        if (sequence.size() == 0) return null;
        List<Solution> solutions = new ArrayList<>();

        for (Indexable r : sequence) {
            Indexable predecessor = table.getTable().pre(r);
            Solution solution = super.evaluate(loadInfo, predecessor, current, target);
            solutions.add(solution);
        }

        return solutions;
    }

    private List<Indexable> getLookForwardSequence(List<LoadInfo> globalLoad, Indexable current, long upperBound) {
        Indexable iterator = current;
        List<Indexable> sequence = new ArrayList<>();
        sequence.add(current);

        // look forward every numOfReplicas-th successor until we find a light node.
        int k = 0;
        for (; k < maxLookForward; k++) {
            Indexable nonReplicaSuccessor = table.getTable().get(iterator.getIndex() + numOfReplicas);
            String nonReplicaSuccessorId = ((VirtualNode) nonReplicaSuccessor).getNodeId();

            // if we already checked all of numOfReplicas-th successors
            // that means no nodes are available for load balancing.
            // so we need to clear the sequence.
            if (nonReplicaSuccessor.getIndex() == current.getIndex()) {
                sequence.clear();
                break;
            }

            // get the load info of the nonReplicaSuccessor
            LoadInfo nonReplicaSuccessorLoadInfo = getLoadInfoOf(nonReplicaSuccessorId, globalLoad);
            if (nonReplicaSuccessorLoadInfo == null) continue;

            // if nonReplicaSuccessor is capable for load balancing, then we find the target
            if (nonReplicaSuccessorLoadInfo.getLoad() < upperBound) break;

            // otherwise, put the nonReplicaSuccessor to the sequence
            // iterate the next nonReplicaSuccessor
            sequence.add(nonReplicaSuccessor);
            iterator = nonReplicaSuccessor;
        }

        // if we reached look forward limit
        // that means no node is available for load balancing
        // clear the sequence.
        if (k == maxLookForward)
            sequence.clear();

        return sequence;
    }

    private LoadInfo getLoadInfoOf(String nonReplicaSuccessorId, List<LoadInfo> globalLoad) {
        LoadInfo result = null;
        for (LoadInfo info : globalLoad) {
            if (info.getNodeId().equals(nonReplicaSuccessorId)) {
                result = info;
                break;
            }
        }

        return result;
    }
}
