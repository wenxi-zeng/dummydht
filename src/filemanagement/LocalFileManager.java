package filemanagement;

import loadmanagement.LoadInfo;
import util.Config;
import util.MathX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class LocalFileManager {

    private Map<Integer, FileBucket> localBuckets;

    private Map<Integer, Gentile> gentiles;

    private BucketMigrateInfo migrateInfo;

    private long numberOfMiss;

    private float readOverhead;

    private float writeOverhead;

    private long interval;

    private long lbUpperBound;

    private static volatile LocalFileManager instance = null;

    private LocalFileManager() {
        localBuckets = new ConcurrentHashMap<>();
        gentiles = new ConcurrentHashMap<>();
        numberOfMiss = 0;
        readOverhead = Config.getInstance().getReadOverhead();
        writeOverhead = Config.getInstance().getWriteOverhead();
        interval = Config.getInstance().getLoadInfoReportInterval() / 1000;
        lbUpperBound = Config.getInstance().getLoadBalancingUpperBound();
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

    public Map<Integer, Gentile> getGentiles() {
        return gentiles;
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
        // load info preparation
        FileBucket dummyBucket = new FileBucket(-1);
        loadInfo.getBucketInfoList().clear();
        loadInfo.setLoadBalancing(false);

        // migrate info preparation
        FileBucket originalBuckets = new FileBucket(-1);
        FileBucket gentileBuckets = new FileBucket(-1);
        Map<String, Long> gentileBucketMap = new HashMap<>();

        for (FileBucket bucket : localBuckets.values()) {
            // load info calculation
            if (bucket.isLocked()) loadInfo.setLoadBalancing(true);
            dummyBucket.merge(bucket);
            loadInfo.getBucketInfoList().add((FileBucket) bucket.clone());

            // migration info calculation
            if (gentiles.containsKey(bucket.getKey())) {
                int key = bucket.getKey();
                Gentile gentile = gentiles.get(key);
                long load = gentileBucketMap.getOrDefault(gentile.getNodeId(), 0L) +
                        bucket.getLoad(readOverhead, writeOverhead, interval);
                gentileBucketMap.put(gentile.getNodeId(), load);
                gentileBuckets.merge(bucket);
            }
            else {
                originalBuckets.merge(bucket);
            }

            bucket.reset();
        }

        // load info finalization
        loadInfo.setSizeOfFiles(dummyBucket.getSizeOfWrites());
        loadInfo.setFileLoad(dummyBucket.getSizeOfWrites());
        loadInfo.setWriteLoad(dummyBucket.getWriteLoad(writeOverhead, interval));
        loadInfo.setReadLoad(dummyBucket.getReadLoad(readOverhead, interval));
        loadInfo.setNumberOfMiss(numberOfMiss);
        loadInfo.setNumberOfLockConflicts(dummyBucket.getNumberOfLockConflicts());
        loadInfo.setNumberOfHits(dummyBucket.getNumberOfReads() +
                dummyBucket.getNumberOfWrites() +
                dummyBucket.getNumberOfLockConflicts() +
                numberOfMiss);

        // migrate info finalization
        BucketMigrateInfo newMigrateInfo = new BucketMigrateInfo(loadInfo.getNodeId());
        newMigrateInfo.setGentileBucketMap(gentileBucketMap);
        newMigrateInfo.setGentileBucketLoad(gentileBuckets.getLoad(readOverhead, writeOverhead, interval));
        newMigrateInfo.setOriginalBucketLoad(originalBuckets.getLoad(readOverhead, writeOverhead, interval));
        newMigrateInfo.setCausedByGentile(newMigrateInfo.getOriginalBucketLoad() < lbUpperBound);
        migrateInfo = newMigrateInfo;

        cleanupGentiles();

        return loadInfo;
    }

    public BucketMigrateInfo getMigrateInfo() {
        return migrateInfo;
    }

    public void addGentile(int bucket, String nodeId) {
        Gentile gentile = new Gentile(nodeId);
        gentiles.put(bucket, gentile);
    }

    public void cleanupGentiles() {
        List<Integer> deadList = new ArrayList<>();
        for (Map.Entry<Integer, Gentile> entry : gentiles.entrySet()) {
            Gentile gentile = entry.getValue();
            gentile.countDown();
            if (gentile.getCounter() < 1) {
                deadList.add(entry.getKey());
            }
        }

        for (Integer key : deadList) {
            gentiles.remove(key);
        }
    }

    private static class Gentile {
        private String nodeId;
        private int counter;

        public Gentile(String nodeId) {
            this.nodeId = nodeId;
            this.counter = 5;
        }

        public void countDown() {
            this.counter--;
        }

        public String getNodeId() {
            return nodeId;
        }

        public int getCounter() {
            return counter;
        }
    }
}
