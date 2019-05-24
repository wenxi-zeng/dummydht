package ceph;

import commonmodels.Indexable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class PlacementGroup implements Indexable {

    private String id;

    private int index;

    private int hash;

    public PlacementGroup() {
    }

    public PlacementGroup(int hash, int index) {
        this.hash = hash;
        this.id = "PG" +  hash;
        this.index = index;
    }

    @Override
    public int getHash() {
        return hash;
    }

    @Override
    public void setHash(int hash) {
        this.hash = hash;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String getDisplayId() {
        return id + "[r" + index + "]";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public int compareTo(Indexable o) {
        return Integer.compare(getHash(), o.getHash());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(id).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof PlacementGroup) == false) {
            return false;
        }
        PlacementGroup rhs = ((PlacementGroup) other);
        return new EqualsBuilder().append(id, rhs.id).isEquals();
    }
}
