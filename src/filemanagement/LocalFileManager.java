package filemanagement;

import loadmanagement.LoadInfo;
import util.MathX;
import util.SimpleLog;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

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
            localBuckets.put(i, new FileBucket(i, MathX.nextInt(1000, 10000), MathX.nextInt(Integer.MAX_VALUE)));
        }
    }

    public FileBucket read(int bucket) {
        return localBuckets.get(bucket);
    }

    public FileBucket write(int bucket) {
        int fileSize = MathX.nextInt(Integer.MAX_VALUE);

        return write(bucket, fileSize);
    }

    public FileBucket write(int bucket, long fileSize) {
        FileBucket fileBucket = localBuckets.get(bucket);

        if (fileBucket == null) {
            fileBucket = new FileBucket(bucket);
            localBuckets.put(bucket, fileBucket);
        }

        fileBucket.setNumberOfFiles(fileBucket.getNumberOfFiles() + 1);
        fileBucket.setSize(fileBucket.getSize() + fileSize);

        SimpleLog.i("File written to bucket [" + bucket + "], file size:" + fileSize);
        return fileBucket;
    }

    public FileBucket write(DummyFile file, Function<String, Integer> func) {
        int bucket = func.apply(file.getName());

        if (file.getSize() < 0) {
            file.setSize(MathX.nextInt(Integer.MAX_VALUE));
        }

        return write(bucket, file.getSize());
    }

    public LoadInfo updateLoadInfo(LoadInfo loadInfo) {
        long fileCount = 0;
        long fileSize = 0;

        for (FileBucket bucket : localBuckets.values()) {
            fileCount += bucket.getNumberOfFiles();
            fileSize += bucket.getSize();
        }

        loadInfo.updateLoad(fileCount, fileSize);
        return loadInfo;
    }
}
