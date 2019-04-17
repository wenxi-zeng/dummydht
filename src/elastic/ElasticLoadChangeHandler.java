package elastic;

import commands.ElasticCommand;
import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import org.apache.commons.lang3.StringUtils;
import util.Config;

import java.util.*;
import java.util.stream.Collectors;

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
        Arrays.sort(fileBuckets);

        Solution bestSolution = null;
        for (LoadInfo targetNodeInfo : lightNodes) {
            Solution sol = evaluate(loadInfo, targetNodeInfo, fileBuckets, lowerBound, upperBound);
            if (bestSolution == null)
                bestSolution = sol;
            else if (!sol.isCauseTargetOverload() && sol.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = sol;
        }

        assert bestSolution != null;
        return new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                .withAttachment(loadInfo.getNodeId() + " " + bestSolution.getTargetNodeId() + " " + StringUtils.join(bestSolution.getBuckets(), ','));
    }

    private Solution evaluate(LoadInfo loadInfo, LoadInfo targetNodeInfo, FileBucket[] fileBuckets, long lowerBound, long upperBound) {
        Solution bestSolution = new Solution(loadInfo.getLoad(), targetNodeInfo.getNodeId());
        List<Solution> solutions = getPossibleSolutions(fileBuckets, loadInfo.getLoad() - lowerBound);

        if (solutions.size() > 0) {
            bestSolution = solutions.get(0);
        }

        for (Solution sol : solutions) {
            sol.setCauseTargetOverload(sol.getResultLoad() + targetNodeInfo.getLoad() > upperBound);
            sol.setResultLoad(loadInfo.getLoad() - sol.getResultLoad());
            sol.setTargetNodeId(targetNodeInfo.getNodeId());

            if (!sol.isCauseTargetOverload() && sol.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = sol;
        }

        return bestSolution;
    }

    private List<Solution> getPossibleSolutions(FileBucket[] fileBuckets, long target) {
        List<Solution> solutions = new ArrayList<>();
        if (fileBuckets.length < 1) return solutions;

        int expanded;
        Stack<Integer> stack = new Stack<>();
        Queue<Integer> queue = new LinkedList<>();
        List<FileBucket> tempList = new ArrayList<>();
        for (int i = 0; i < fileBuckets.length; i++) {
            queue.add(i);
        }
        //noinspection ConstantConditions
        expanded = queue.poll();
        stack.push(expanded);

        while (!stack.empty()) {
            if(target - getLoad(fileBuckets[expanded]) < 0) {
                Solution solution = new Solution();
                solution.setBuckets(tempList.stream().map(FileBucket::getKey).collect(Collectors.toList()));
                solution.setResultLoad(target);
                solutions.add(solution);

                if (!queue.isEmpty()) { // if empty, will be removed later
                    stack.pop();
                    int last = tempList.size() - 1;
                    if (last > -1) {
                        target += getLoad(tempList.get(last));
                        tempList.remove(last);
                    }
                }
            }
            else {
                target -= getLoad(fileBuckets[expanded]);
                tempList.add(fileBuckets[expanded]);
            }

            if (queue.isEmpty()) { // empty, remove what is in the tempList
                stack.pop();
                if (stack.empty()) break;
                int last = tempList.size() - 1;
                if (last > -1) {
                    target += getLoad(tempList.get(last));
                    tempList.remove(last);
                }
                for (int i = stack.peek() + 1; i < fileBuckets.length; i++) {
                    queue.add(i);
                }
            }
            else {
                expanded = queue.poll();
                stack.push(expanded);
            }
        }

        return solutions;
    }

    public double getLoad(FileBucket bucket) {
        return ((writeOverhead * bucket.getNumberOfWrites() + bucket.getSizeOfWrites()) * 1.0) / interval +
                ((readOverhead * bucket.getNumberOfReads() + bucket.getSizeOfReads()) * 1.0) / interval;
    }

    private class Solution {
        private List<Integer> buckets;
        private long resultLoad;
        private String targetNodeId;
        private boolean causeTargetOverload;

        public Solution() {
            causeTargetOverload = false;
        }

        public Solution(long initLoad, String targetNodeId) {
            buckets = new ArrayList<>();
            resultLoad = initLoad;
            causeTargetOverload = false;
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

        public boolean isCauseTargetOverload() {
            return causeTargetOverload;
        }

        public void setCauseTargetOverload(boolean causeTargetOverload) {
            this.causeTargetOverload = causeTargetOverload;
        }
    }
}
