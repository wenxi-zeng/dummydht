package elastic;

import commands.ElasticCommand;
import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import org.apache.commons.lang3.StringUtils;
import util.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        List<LoadInfo> lightNodes = new ArrayList<>();
        for (LoadInfo info : globalLoad) {
            if (info.getLoad() < lowerBound) {
                lightNodes.add(info);
            }
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

        Solution bestSolution = null;
        for (LoadInfo targetNodeInfo : lightNodes) {
            Solution sol = evaluate(loadInfo, targetNodeInfo, fileBuckets, lowerBound, upperBound);
            if (sol == null) continue;
            if (bestSolution == null)
                bestSolution = sol;
            else if (sol.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = sol;
        }

        assert bestSolution != null;
        return new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                .withAttachment(loadInfo.getNodeId() + " " + bestSolution.getTargetNodeId() + " " + StringUtils.join(bestSolution.getBuckets(), ','));
    }

    private Solution evaluate(LoadInfo loadInfo, LoadInfo targetNodeInfo, FileBucket[] fileBuckets, long lowerBound, long upperBound) {
        if (fileBuckets.length < 1) return null;

        long target = loadInfo.getLoad() - lowerBound;
        long accumulated = 0;
        Solution solution = new Solution();
        List<Integer> selectedBuckets = new ArrayList<>();

        for (FileBucket bucket : fileBuckets) {
            if (target < 0) break;
            double load = getLoad(bucket);
            if (target > load && accumulated + targetNodeInfo.getLoad() < upperBound) {
                selectedBuckets.add(bucket.getKey());
                target -= load;
                accumulated += load;
            }
        }
        solution.setBuckets(selectedBuckets);
        solution.setResultLoad(loadInfo.getLoad() - accumulated);
        solution.setTargetNodeId(targetNodeInfo.getNodeId());

        return solution;
    }

    public double getLoad(FileBucket bucket) {
        return ((writeOverhead * bucket.getNumberOfWrites() + bucket.getSizeOfWrites()) * 1.0) / interval +
                ((readOverhead * bucket.getNumberOfReads() + bucket.getSizeOfReads()) * 1.0) / interval;
    }

    private class Solution {
        private List<Integer> buckets;
        private long resultLoad;
        private String targetNodeId;

        public Solution() {
        }

        public Solution(long initLoad, String targetNodeId) {
            buckets = new ArrayList<>();
            resultLoad = initLoad;
            this.targetNodeId = targetNodeId;
        }

        public List<Integer> getBuckets() {
            return buckets;
        }

        public void setBuckets(List<Integer> buckets) {
            this.buckets = buckets;
        }

        public long getResultLoad() {
            return resultLoad;
        }

        public String getTargetNodeId() {
            return targetNodeId;
        }

        public void setTargetNodeId(String targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        public void setResultLoad(long resultLoad) {
            this.resultLoad = resultLoad;
        }
    }
}
