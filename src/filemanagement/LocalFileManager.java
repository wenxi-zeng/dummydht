package filemanagement;

import util.MathX;
import util.SimpleLog;

import java.util.HashMap;
import java.util.Map;

public class LocalFileManager {

    private Map<Integer, FileBucket> localBuckets;

    private static volatile LocalFileManager instance = null;

    private LocalFileManager() {
        localBuckets = new HashMap<>();
    }

    public static LocalFileManager getInstance() {
        if (instance == null) {
            synchronized(LocalFileManager.class) {
                if (instance == null) {
                    instance = new LocalFileManager();
                }
            }
        }

        return instance;
    }

    public Map<Integer, FileBucket> getLocalBuckets() {
        return localBuckets;
    }

    public void setLocalBuckets(Map<Integer, FileBucket> localBuckets) {
        this.localBuckets = localBuckets;
    }

    public void generateFileBuckets(int numberOfBuckets) {
        for (int i = 0; i < numberOfBuckets; i++) {
            localBuckets.put(i, new FileBucket(i, MathX.NextInt(1000, 10000), MathX.NextInt(Integer.MAX_VALUE)));
        }
    }

    public void write(int bucket) {
        int fileSize = MathX.NextInt(Integer.MAX_VALUE);

        FileBucket fileBucket = localBuckets.get(bucket);
        fileBucket.setNumberOfFiles(fileBucket.getNumberOfFiles() + 1);
        fileBucket.setSize(fileBucket.getSize() + fileSize);

        SimpleLog.i("File written to bucket [" + bucket + "], file size:" + fileSize);
    }
}
