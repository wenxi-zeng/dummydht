package algorithms.loadbalance;

import models.Indexable;
import models.LookupTable;
import models.PhysicalNode;

public interface LoadBalanceAlgorithm {

    void increaseLoad(LookupTable table, PhysicalNode node);

    void decreaseLoad(LookupTable table, PhysicalNode node);

    void decreaseLoad(LookupTable table, int dh, Indexable node);

    void increaseLoad(LookupTable table, int dh, Indexable node);

    void nodeJoin(LookupTable table, Indexable node);

    void nodeLeave(LookupTable table, Indexable node);

}
