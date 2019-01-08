package filemanagement;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class FileBucket implements Serializable {

    private int key;

    private int numberOfFiles;

    private long size;

    private boolean locked;

    public FileBucket(int key) {
        this.key = key;
        this.locked = false;
    }

    public FileBucket(int key, int numberOfFiles, long size) {
        this(key);
        this.numberOfFiles = numberOfFiles;
        this.size = size;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getNumberOfFiles() {
        return numberOfFiles;
    }

    public void setNumberOfFiles(int numberOfFiles) {
        this.numberOfFiles = numberOfFiles;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("key", key).append("numberOfFiles", numberOfFiles).append("size", size).append("locked", locked).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(numberOfFiles).append(locked).append(key).append(size).toHashCode();
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
        return new EqualsBuilder().append(numberOfFiles, rhs.numberOfFiles).append(locked, rhs.locked).append(key, rhs.key).append(size, rhs.size).isEquals();
    }
}
