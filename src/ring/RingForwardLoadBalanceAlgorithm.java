package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import util.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RingForwardLoadBalanceAlgorithm extends RingLoadBalanceAlgorithm {

    private final float readOverhead;

    private final float writeOverhead;

    private final long interval;

    public RingForwardLoadBalanceAlgorithm() {
        readOverhead = Config.getInstance().getReadOverhead();
        writeOverhead = Config.getInstance().getWriteOverhead();
        interval = Config.getInstance().getLoadInfoReportInterval() / 1000;
    }

    public void decreaseLoad(LookupTable table, PhysicalNode node, Indexable vnode, long delta) {
        List<Integer> hashVal = new ArrayList<>();
        Indexable predecessor = table.getTable().pre(vnode);
        int iterator = vnode.getHash();
        int start = predecessor.getHash() + 1; // the hash of predecessor needs to be excluded
        Map<Integer, FileBucket> map = LocalFileManager.getInstance().getLocalBuckets();
        while (inRange(iterator, start, vnode.getHash())) {
            FileBucket bucket = map.get(iterator--);
            if (bucket == null) break;
            long load = bucket.getLoad(readOverhead, writeOverhead, interval);
            if (delta - load < 0)
                break;

            hashVal.add(bucket.getKey());
            delta -= load;
            if (iterator < 0) iterator = Config.getInstance().getNumberOfHashSlots() - 1;
        }

        decreaseLoad(table, node, hashVal.stream().mapToInt(Integer::intValue).toArray());
    }

}
