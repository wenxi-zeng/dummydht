package algorithms.loadbalance;

import models.Indexable;

public interface LoadBalanceAlgorithm {

    void decreaseLoad(int dh, Indexable node);

    void increaseLoad(int dh, Indexable node);

    void nodeJoin(Indexable node);

    void nodeLeave(Indexable node);

}
