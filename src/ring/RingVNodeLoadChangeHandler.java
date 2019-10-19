package ring;

import commonmodels.Indexable;
import commonmodels.LoadChangeHandler;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import org.apache.commons.lang3.StringUtils;
import util.Config;

import java.util.*;
import java.util.stream.Collectors;

public class RingVNodeLoadChangeHandler implements LoadChangeHandler {

    private final LookupTable table;

    private float readOverhead;

    private float writeOverhead;

    private long interval;

    public RingVNodeLoadChangeHandler(LookupTable table) {
        this.table = table;
        readOverhead = Config.getInstance().getReadOverhead();
        writeOverhead = Config.getInstance().getWriteOverhead();
        interval = Config.getInstance().getLoadInfoReportInterval() / 1000;
    }

    @Override
    public List<Request> generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        PhysicalNode node = new PhysicalNode(loadInfo.getNodeId());
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null || pnode.getVirtualNodes() == null) return null;

        List<LoadInfo> sortedTargets = sortTargets(globalLoad, loadInfo, lowerBound, upperBound);
        if (sortedTargets == null) return null;

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            Indexable predecessor = table.getTable().pre(vnode);
            Solution solution = evaluate(loadInfo, predecessor, vnode, target);

            if (bestSolution == null || solution.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = solution;
        }
        List<FileBucket> temp = new ArrayList<>();
        for (FileBucket bucket : loadInfo.getBucketInfoList()) {
            double load = getLoad(bucket);
            if (load > 0 && load < upperBound) // filter empty buckets and overloaded buckets
                temp.add(bucket);
        }
        FileBucket[] fileBuckets = new FileBucket[temp.size()];
        fileBuckets = temp.toArray(fileBuckets);
        Arrays.sort(fileBuckets, (o1, o2) -> -1 * Double.compare(getLoad(o1), getLoad(o2)));

        long target = computeTargetLoad(globalLoad, loadInfo, lowerBound, upperBound);
        List<Solution> solutions = evaluate(sortedTargets, loadInfo, fileBuckets, target, upperBound);
        List<Request> requests = new ArrayList<>();
        if (solutions != null) {
            if (solutions.size() < 1) {
                requests.add(new Request()
                        .withHeader(ElasticCommand.EXPAND.name())
                        .withAttachment(String.valueOf(table.getTable().length * 2)));
            }
            else {
                for (Solution solution : solutions) {
                    requests.add(new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                            .withReceiver(loadInfo.getNodeId())
                            .withAttachment(loadInfo.getNodeId() + " " + solution.getTargetNodeId() + " " + StringUtils.join(solution.getVnodes(), ',')));
                }
            }
        }

        return requests;
    }

    @Override
    public void optimize(List<Request> requests) {
        // stub
    }

    @Override
    public long computeTargetLoad(List<LoadInfo> loadInfoList, LoadInfo loadInfo, long lowerBound, long upperBound) {
        return lowerBound;
    }

    private List<Solution> evaluate(List<LoadInfo> sortedTargets, LoadInfo loadInfo, List<Indexable> vnodes, long lowerBound, long upperBound) {
        if (vnodes.size() < 1) return null;

        long target = loadInfo.getLoad() - lowerBound;
        Map<String, Solution> solutions = new HashMap<>();
        Map<Integer, FileBucket> map = loadInfo.getBucketInfoList().stream().collect(
                Collectors.toMap(FileBucket::getKey, bucket -> bucket, FileBucket::merge));

        for (Indexable vnode : vnodes) {
            if (target < 0) break;
            Indexable predecessor = table.getTable().pre(vnode);
            double load = getLoad(map, vnode, predecessor);
            for (LoadInfo targetNode : sortedTargets) {
                Solution solution = new Solution();
                if (load + targetNode.getLoad() < upperBound) {
                    solution.addVnode(vnode.getHash(), load);
                    solution.setTargetNodeId(targetNode.getNodeId());
                    target -= load;
                    solutions.put(targetNode.getNodeId(), solution);
                    break;
                }
            }
        }

        return new ArrayList<>(solutions.values());
    }

    private List<LoadInfo> sortTargets(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        List<LoadInfo> veryLightNodes = new ArrayList<>();
        List<LoadInfo> medianLightNodes = new ArrayList<>();
        List<LoadInfo> lightNodes = new ArrayList<>();
        List<LoadInfo> result = new ArrayList<>();

        for (LoadInfo info : globalLoad) {
            if (info.getLoad() < 0.3 * lowerBound) {
                veryLightNodes.add(info);
            }
            else if (info.getLoad() < 0.6 * lowerBound) {
                medianLightNodes.add(info);
            }
            else {
                lightNodes.add(info);
            }
        }
        veryLightNodes.sort(Comparator.comparingLong(LoadInfo::getLoad));
        medianLightNodes.sort(Comparator.comparingLong(LoadInfo::getLoad));
        lightNodes.sort(Comparator.comparingLong(LoadInfo::getLoad));

        int loadLevel = loadInfo.getLoadLevel(lowerBound, upperBound);
        if (loadLevel == LoadInfo.LEVEL_HEAVY) {
            result.addAll(lightNodes);
            result.addAll(medianLightNodes);
            result.addAll(veryLightNodes);
            return result;
        }
        else if (loadLevel == LoadInfo.LEVEL_MEDIAN_HEAVY) {
            result.addAll(medianLightNodes);
            result.addAll(veryLightNodes);
            result.addAll(lightNodes);
            return result;
        }
        else if (loadLevel == LoadInfo.LEVEL_VERY_HEAVY) {
            result.addAll(veryLightNodes);
            result.addAll(medianLightNodes);
            result.addAll(lightNodes);
            return result;
        }
        else {
            return null;
        }
    }

    private double getLoad(Map<Integer, FileBucket> map, Indexable current, Indexable predecessor) {
        int iterator = current.getHash();
        int start = predecessor.getHash() + 1; // the hash of predecessor needs to be excluded
        double load = 0;
        while (inRange(iterator, start, current.getHash())) {
            FileBucket bucket = map.get(iterator--);
            if (bucket != null)
                load += bucket.getLoad(readOverhead, writeOverhead, interval);

            if (iterator < 0) iterator = Config.getInstance().getNumberOfHashSlots() - 1;
        }

        return load;
    }

    private boolean inRange(int bucket, int start, int end) {
        if (start > end) {
            return (bucket > start && bucket < Config.getInstance().getNumberOfHashSlots()) ||
                    (bucket >= 0 && bucket <= end);
        }
        else {
            return bucket > start && bucket <= end;
        }
    }

    private class Solution {
        private List<Integer> vnodes;
        private double loadToMove;
        private String targetNodeId;

        public Solution() {
            vnodes = new ArrayList<>();
        }

        public void addVnode(int vnode, double load) {
            vnodes.add(vnode);
            loadToMove += load;
        }

        public List<Integer> getVnodes() {
            return vnodes;
        }

        public void setVnodes(List<Integer> vnodes) {
            this.vnodes = vnodes;
        }

        public String getTargetNodeId() {
            return targetNodeId;
        }

        public void setTargetNodeId(String targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        public double getLoadToMove() {
            return loadToMove;
        }

        public void setLoadToMove(double loadToMove) {
            this.loadToMove = loadToMove;
        }
    }
}
