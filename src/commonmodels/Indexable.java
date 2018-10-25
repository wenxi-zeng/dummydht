package commonmodels;

public interface Indexable extends Comparable<Indexable>{
    int getHash();

    void setHash(int hash);

    int getIndex();

    void setIndex(int index);

    String getDisplayId();
}
