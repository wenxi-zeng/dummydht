package filemanagement;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class FileBucket implements Serializable, Comparable<FileBucket> {

    private int key;

    private long sizeOfReads;

    private long sizeOfWrites;

    private long numberOfReads;

    private long numberOfWrites;

    private long numberOfLockConflicts;

    private boolean locked;

    public FileBucket(int key) {
        this.key = key;
        this.locked = false;
    }

    public FileBucket(int key, int numberOfFiles, long size) {
        this(key);
        this.numberOfWrites = numberOfFiles;
        this.sizeOfWrites = size;
    }

    public void merge(FileBucket bucket) {
        this.numberOfWrites += bucket.numberOfWrites;
        this.sizeOfWrites += bucket.sizeOfWrites;
        this.numberOfReads += bucket.numberOfReads;
        this.numberOfWrites += bucket.numberOfWrites;
    }

    public void write(long filesize) {
        if (locked) {
            this.numberOfLockConflicts++;
        }
        else {
            this.numberOfWrites++;
            this.sizeOfWrites += filesize;
        }
    }

    public FileBucket read() {
        this.numberOfReads++;
        return this;
    }

    public FileBucket read(long filesize) {
        this.numberOfReads++;
        this.sizeOfReads += filesize;
        return this;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public long getSizeOfReads() {
        return sizeOfReads;
    }

    public void setSizeOfReads(long sizeOfReads) {
        this.sizeOfReads = sizeOfReads;
    }

    public long getSizeOfWrites() {
        return sizeOfWrites;
    }

    public void setSizeOfWrites(long sizeOfWrites) {
        this.sizeOfWrites = sizeOfWrites;
    }

    public long getNumberOfReads() {
        return numberOfReads;
    }

    public void setNumberOfReads(long numberOfReads) {
        this.numberOfReads = numberOfReads;
    }

    public long getNumberOfWrites() {
        return numberOfWrites;
    }

    public void setNumberOfWrites(long numberOfWrites) {
        this.numberOfWrites = numberOfWrites;
    }

    public long getNumberOfLockConflicts() {
        return numberOfLockConflicts;
    }

    public void setNumberOfLockConflicts(long numberOfLockConflicts) {
        this.numberOfLockConflicts = numberOfLockConflicts;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("key", key).append("sizeOfWrites", sizeOfWrites).append("sizeOfReads", sizeOfReads).append("locked", locked).append("numberOfReads", numberOfReads).append("numberOfWrites", numberOfWrites).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(locked).append(key).append(sizeOfWrites).append(sizeOfReads).append(numberOfReads).append(numberOfWrites).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof FileBucket) == false) {
            return false;
        }
        FileBucket rhs = ((FileBucket) other);
        return new EqualsBuilder().append(locked, rhs.locked).append(key, rhs.key).append(sizeOfWrites, rhs.sizeOfWrites).append(sizeOfReads, rhs.sizeOfReads).append(numberOfReads, rhs.numberOfReads).append(numberOfWrites, rhs.numberOfWrites).isEquals();
    }

    @Override
    public int compareTo(FileBucket o) {
        return Long.compare(this.sizeOfWrites, o.sizeOfWrites);
    }
}
