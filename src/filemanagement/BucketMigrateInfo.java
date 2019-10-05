package filemanagement;

import commonmodels.Queueable;

import java.util.HashMap;
import java.util.Map;

public class BucketMigrateInfo implements Queueable {

    private String nodeId;

    private long originalBucketLoad;

    private Map<String, Long> gentileBucketMap;

    private long gentileBucketLoad;

    private boolean causedByGentile;

    public BucketMigrateInfo(String nodeId) {
        this.nodeId = nodeId;
        this.originalBucketLoad = 0;
        this.gentileBucketMap = new HashMap<>();
        this.gentileBucketLoad = 0;
        this.causedByGentile = false;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public long getOriginalBucketLoad() {
        return originalBucketLoad;
    }

    public void setOriginalBucketLoad(long originalBucketLoad) {
        this.originalBucketLoad = originalBucketLoad;
    }

    public Map<String, Long> getGentileBucketMap() {
        return gentileBucketMap;
    }

    public void setGentileBucketMap(Map<String, Long> gentileBucketMap) {
        this.gentileBucketMap = gentileBucketMap;
    }

    public long getGentileBucketLoad() {
        return gentileBucketLoad;
    }

    public void setGentileBucketLoad(long gentileBucketLoad) {
        this.gentileBucketLoad = gentileBucketLoad;
    }

    public boolean isCausedByGentile() {
        return causedByGentile;
    }

    public void setCausedByGentile(boolean causedByGentile) {
        this.causedByGentile = causedByGentile;
    }
}
