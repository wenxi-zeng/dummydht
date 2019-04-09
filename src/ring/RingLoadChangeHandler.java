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
        Solution bestSolution = null;

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            Indexable vnode = pnode.getVirtualNodes().get(i);
            Indexable predecessor = table.getTable().pre(vnode);
            Solution solution = evaluate(loadInfo, predecessor, vnode, lowerBound);

            if (bestSolution == null || solution.getResultLoad() < bestSolution.getResultLoad())
                bestSolution = solution;
        }

        if (bestSolution == null) return null;

        List<Integer> deltaHashList = new ArrayList<>();
        for (Indexable vnode : pnode.getVirtualNodes()) {
            if (vnode.getHash() == bestSolution.getVnodeHash())
                deltaHashList.add(bestSolution.getDelta());
            else
                deltaHashList.add(0);
        }

        return new Request().withHeader(RingCommand.DECREASELOAD.name())
                .withAttachments(loadInfo.getNodeId(), StringUtils.join(deltaHashList, ','));
    }

    private Solution evaluate(LoadInfo loadInfo, Indexable predecessor, Indexable current, long lowerBound) {
        Solution solution = new Solution(loadInfo.getLoad(), current.getHash());
        Map<Integer, FileBucket> map = loadInfo.getBucketInfoList().stream().collect(
                Collectors.toMap(FileBucket::getKey, bucket -> bucket));

        int iterator = current.getHash();
        while (inRange(iterator, predecessor.getHash(), current.getHash())) {
            FileBucket bucket = map.get(iterator--);
            if (bucket != null && !solution.update(bucket.getKey(), bucket.getSizeOfWrites(), lowerBound))
                break;

            if (iterator < 0) iterator = Config.getInstance().getNumberOfHashSlots() - 1;
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
        private int delta;
        private int vnodeHash;

        public Solution(long initLoad, int vnodeHash) {
            buckets = new ArrayList<>();
            resultLoad = initLoad;
            delta = 0;
            this.vnodeHash = vnodeHash;
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

        public int getDelta() {
            return delta;
        }

        public void setDelta(int delta) {
            this.delta = delta;
        }

        public int getVnodeHash() {
            return vnodeHash;
        }

        public void setVnodeHash(int vnodeHash) {
            this.vnodeHash = vnodeHash;
        }

        public boolean update(int bucket, long load, long target) {
            long temp = resultLoad - load;
            if (temp < target) {
                return false;
            }
            else {
                buckets.add(bucket);
                resultLoad -= load;
                delta++;
                return true;
            }
        }
    }
}
