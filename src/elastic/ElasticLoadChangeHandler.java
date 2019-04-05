package elastic;

import commands.ElasticCommand;
import commonmodels.LoadChangeHandler;
import commonmodels.transport.Request;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticLoadChangeHandler implements LoadChangeHandler {

    private final LookupTable table;

    public ElasticLoadChangeHandler(LookupTable table) {
        this.table = table;
    }

    @Override
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        List<LoadInfo> lightNodes = new ArrayList<>();
        for (LoadInfo info : globalLoad) {
            if (info.getLoad() < lowerBound) {
                lightNodes.add(info);
            }
        }

        FileBucket[] fileBuckets = new FileBucket[loadInfo.getBucketInfoList().size()];
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
        Solution bestSolution = new Solution(loadInfo.getFileLoad(), targetNodeInfo.getNodeId());
        List<Solution> solutions = getPossibleSolutions(fileBuckets, loadInfo.getFileLoad() - lowerBound);

        if (solutions.size() > 0) {
            bestSolution = solutions.get(0);
        }

        for (Solution sol : solutions) {
            sol.setCauseTargetOverload(sol.getResultLoad() + targetNodeInfo.getFileLoad() > upperBound);
            sol.setResultLoad(loadInfo.getFileLoad() - sol.getResultLoad());
            sol.setTargetNodeId(targetNodeInfo.getNodeId());

            if (!sol.isCauseTargetOverload() && sol.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = sol;
        }

        return bestSolution;
    }

    public List<Solution> getPossibleSolutions(FileBucket[] fileBuckets, long target) {
        List<Solution> list = new ArrayList<>();
        backtrack(list, new ArrayList<>(), fileBuckets, target, 0);
        return list;
    }

    private void backtrack(List<Solution> list, List<FileBucket> tempList, FileBucket[] fileBuckets, long remain, int start){
        if(start >= fileBuckets.length) return;
        else if(remain - fileBuckets[start].getSizeOfWrites() < 0) {
            Solution solution = new Solution();
            solution.setBuckets(tempList.stream().map(FileBucket::getKey).collect(Collectors.toList()));
            solution.setResultLoad(remain);
            list.add(solution);
        }
        else{
            for(int i = start; i < fileBuckets.length; i++){
                tempList.add(fileBuckets[i]);
                backtrack(list, tempList, fileBuckets, remain - fileBuckets[i].getSizeOfWrites(), i); // not i + 1 because we can reuse same elements
                tempList.remove(tempList.size() - 1);
            }
        }
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
