package elastic;

import commands.ElasticCommand;
import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import org.apache.commons.lang3.StringUtils;
import util.Config;

import java.util.*;

public class ElasticLoadChangeHandler implements LoadChangeHandler {

    private final LookupTable table;

    private float readOverhead;

    private float writeOverhead;

    private long interval;

    public ElasticLoadChangeHandler(LookupTable table) {
        this.table = table;
        readOverhead = Config.getInstance().getReadOverhead();
        writeOverhead = Config.getInstance().getWriteOverhead();
        interval = Config.getInstance().getLoadInfoReportInterval() / 1000;
    }

    @Override
    public List<Request> generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        List<LoadInfo> sortedTargets = sortTargets(globalLoad, loadInfo, lowerBound, upperBound);
        if (sortedTargets == null) return null;

        List<FileBucket> temp = new ArrayList<>();
        for (FileBucket bucket : loadInfo.getBucketInfoList()) {
            double load = getLoad(bucket);
            if (load > 0 && load < upperBound) // filter empty buckets and overloaded buckets
                temp.add(bucket);
        }
        FileBucket[] fileBuckets = new FileBucket[temp.size()];
        fileBuckets = temp.toArray(fileBuckets);
        Arrays.sort(fileBuckets, (o1, o2) -> -1 * Double.compare(getLoad(o1), getLoad(o2)));

        List<Solution> solutions = evaluate(sortedTargets, loadInfo, fileBuckets, lowerBound, upperBound);
        List<Request> requests = new ArrayList<>();
        if (solutions != null) {
            for (Solution solution : solutions) {
                requests.add(new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                        .withAttachment(loadInfo.getNodeId() + " " + solution.getTargetNodeId() + " " + StringUtils.join(solution.getBuckets(), ',')));
            }
        }

        return requests;
    }

    @Override
    public void optimize(List<Request> requests) {
        // stub
    }

    private List<Solution> evaluate(List<LoadInfo> sortedTargets, LoadInfo loadInfo, FileBucket[] fileBuckets, long lowerBound, long upperBound) {
        if (fileBuckets.length < 1) return null;

        long target = loadInfo.getLoad() - lowerBound;
        Map<String, Solution> solutions = new HashMap<>();

        for (FileBucket bucket : fileBuckets) {
            if (target < 0) break;
            double load = getLoad(bucket);
            if (target > load) {
                for (LoadInfo targetNode : sortedTargets) {
                    Solution solution = solutions.getOrDefault(targetNode.getNodeId(), new Solution());
                    if (solution.getLoadToMove() + targetNode.getLoad() < upperBound) {
                        solution.addBucket(bucket.getKey(), load);
                        solution.setTargetNodeId(targetNode.getNodeId());
                        target -= load;
                        solutions.put(targetNode.getNodeId(), solution);
                        break;
                    }
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

    private double getLoad(FileBucket bucket) {
        return ((writeOverhead * bucket.getNumberOfWrites() + bucket.getSizeOfWrites()) * 1.0) / interval +
                ((readOverhead * bucket.getNumberOfReads() + bucket.getSizeOfReads()) * 1.0) / interval;
    }

    private class Solution {
        private List<Integer> buckets;
        private double loadToMove;
        private String targetNodeId;

        public Solution() {
            buckets = new ArrayList<>();
        }

        public void addBucket(int bucket, double load) {
            buckets.add(bucket);
            loadToMove += load;
        }

        public List<Integer> getBuckets() {
            return buckets;
        }

        public void setBuckets(List<Integer> buckets) {
            this.buckets = buckets;
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
