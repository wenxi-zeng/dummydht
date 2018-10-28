package filemanagement;

public class FileBucket {

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
}
