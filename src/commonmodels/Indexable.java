package commonmodels;

import java.io.Serializable;

public interface Indexable extends Comparable<Indexable>, Serializable {
    int getHash();

    void setHash(int hash);

    int getIndex();

    void setIndex(int index);

    String getDisplayId();
}
