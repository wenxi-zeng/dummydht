package filemanagement;

import commonmodels.Queueable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BucketMigrateInfo implements Queueable {

    private String nodeId;

    private long originalBucketLoad;

    private Map<String, Long> gentileBucketMap;

    private long gentileBucketLoad;

    private boolean causedByGentile;

    private String token;

    private long timestamp;

    public BucketMigrateInfo(String nodeId) {
        this.nodeId = nodeId;
        this.originalBucketLoad = 0;
        this.gentileBucketMap = new HashMap<>();
        this.gentileBucketLoad = 0;
        this.causedByGentile = false;
        this.timestamp = System.currentTimeMillis();
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

    public String getGentileBucketMapString() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Long> entry : gentileBucketMap.entrySet()) {
            builder.append(entry.getKey()).append("-").append(entry.getValue()).append(";");
        }
        return builder.toString();
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
