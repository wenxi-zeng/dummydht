package ring;

import commands.RingCommand;
import commonmodels.Indexable;
import commonmodels.LoadChangeHandler;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import org.apache.commons.lang3.StringUtils;
import util.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RingLoadChangeHandler implements LoadChangeHandler {

    private final LookupTable table;

    public RingLoadChangeHandler(LookupTable table) {
        this.table = table;
    }

    @Override
    public Request generateRequestBasedOnLoad(List<LoadInfo> globalLoad, LoadInfo loadInfo, long lowerBound, long upperBound) {
        PhysicalNode node = new PhysicalNode(loadInfo.getNodeId());
        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        Solution bestSolution = new Solution(loadInfo.getFileLoad());

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            Indexable predecessor = table.getTable().pre(vnode);
            Solution solution = evaluate(loadInfo, predecessor, vnode, lowerBound);

            if (solution.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = solution;
        }

        return new Request().withHeader(RingCommand.DECREASELOAD.name())
                .withAttachments(loadInfo.getNodeId(), StringUtils.join(bestSolution.getBuckets(), ','));
    }

    private Solution evaluate(LoadInfo loadInfo, Indexable predecessor, Indexable current, long lowerBound) {
        Solution solution = new Solution(loadInfo.getFileLoad());
        Map<Integer, FileBucket> map = loadInfo.getBucketInfoList().stream().collect(
                Collectors.toMap(FileBucket::getKey, bucket -> bucket));

        int iterator = current.getHash();
        while (inRange(iterator, predecessor.getHash(), current.getHash())) {
            FileBucket bucket = map.get(iterator);
            if (!solution.update(bucket.getKey(), bucket.getSizeOfWrites(), lowerBound))
                break;
        }

        return solution;
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
        private List<Integer> buckets;
        private long resultLoad;

        public Solution(long initLoad) {
            buckets = new ArrayList<>();
            resultLoad = initLoad;
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

        public void setResultLoad(long resultLoad) {
            this.resultLoad = resultLoad;
        }

        public boolean update(int bucket, long load, long target) {
            long temp = resultLoad - load;
            if (temp < target) {
                return false;
            }
            else {
                buckets.add(bucket);
                resultLoad -= load;
                return true;
            }
        }
    }
}
