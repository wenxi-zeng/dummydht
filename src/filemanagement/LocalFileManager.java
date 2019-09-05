package filemanagement;

import loadmanagement.LoadInfo;
import util.Config;
import util.MathX;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class LocalFileManager {

    private Map<Integer, FileBucket> localBuckets;

    private long numberOfMiss;

    private float readOverhead;

    private float writeOverhead;

    private long interval;

    private static volatile LocalFileManager instance = null;

    private LocalFileManager() {
        localBuckets = new HashMap<>();
        numberOfMiss = 0;
        readOverhead = Config.getInstance().getReadOverhead();
        writeOverhead = Config.getInstance().getWriteOverhead();
        interval = Config.getInstance().getLoadInfoReportInterval() / 1000;
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

    public static void deleteInstance() {
        instance = null;
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

    public FileBucket read(int bucket, long filesize) {
        if (filesize < 0) return read(bucket);

        FileBucket fileBucket = localBuckets.get(bucket);

        if (fileBucket != null) {
            return fileBucket.read(filesize);
        }
        else {
            numberOfMiss++;
            return null;
        }
    }

    public FileBucket read(int bucket) {
        FileBucket fileBucket = localBuckets.get(bucket);

        if (fileBucket != null) {
            return fileBucket.read();
        }
        else {
            numberOfMiss++;
            return null;
        }
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

        fileBucket.write(fileSize);

        //SimpleLog.i("File written to bucket [" + bucket + "], file size:" + fileSize);
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
        FileBucket dummyBucket = new FileBucket(-1);
        loadInfo.getBucketInfoList().clear();
        loadInfo.setLoadBalancing(false);

        for (FileBucket bucket : localBuckets.values()) {
            if (bucket.isLocked()) loadInfo.setLoadBalancing(true);
            dummyBucket.merge(bucket);
            loadInfo.getBucketInfoList().add((FileBucket) bucket.clone());
            bucket.reset();
        }

        loadInfo.setSizeOfFiles(dummyBucket.getSizeOfWrites());
        loadInfo.setFileLoad(dummyBucket.getSizeOfWrites());
        loadInfo.setWriteLoad((long)(((writeOverhead * dummyBucket.getNumberOfWrites() + dummyBucket.getSizeOfWrites()) * 1.0) / interval));
        loadInfo.setReadLoad((long)(((readOverhead * dummyBucket.getNumberOfReads() + dummyBucket.getSizeOfReads()) * 1.0) / interval));
        loadInfo.setNumberOfMiss(numberOfMiss);
        loadInfo.setNumberOfLockConflicts(dummyBucket.getNumberOfLockConflicts());
        loadInfo.setNumberOfHits(dummyBucket.getNumberOfReads() +
                dummyBucket.getNumberOfWrites() +
                dummyBucket.getNumberOfLockConflicts() +
                numberOfMiss);
        return loadInfo;
    }
}
