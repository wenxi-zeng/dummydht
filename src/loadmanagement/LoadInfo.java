package loadmanagement;

import commonmodels.Queueable;
import commonmodels.Transportable;
import filemanagement.FileBucket;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LoadInfo extends Transportable implements Serializable, Queueable
{
    private long reportTime;
    private String nodeId;
    private long fileLoad;
    private long sizeOfFiles;
    private long readLoad;
    private long writeLoad;
    private long numberOfMiss;
    private long numberOfLockConflicts;
    private long numberOfHits;
    private final static long serialVersionUID = -607052110101864782L;

    private List<FileBucket> bucketInfoList = new ArrayList<>();

    private transient boolean consolidated = false;

    private transient boolean loadBalancing = false;

    private transient int serialNumber;

    public final static int LEVEL_LIGHT = 0;
    public final static int LEVEL_MEDIAN_LIGHT = 1;
    public final static int LEVEL_VERY_LIGHT = 2;
    public final static int LEVEL_NORMAL = 3;
    public final static int LEVEL_HEAVY = 4;
    public final static int LEVEL_MEDIAN_HEAVY = 5;
    public final static int LEVEL_VERY_HEAVY = 6;

    /**
     * No args constructor for use in serialization
     *
     */
    public LoadInfo() {
    }

    public long getReportTime() {
        return reportTime;
    }

    public void setReportTime(long reportTime) {
        this.reportTime = reportTime;
    }

    public LoadInfo withReportTime(long reportTime) {
        this.reportTime = reportTime;
        return this;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public LoadInfo withNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public long getFileLoad() {
        return fileLoad;
    }

    public void setFileLoad(long fileLoad) {
        this.fileLoad = fileLoad;
    }

    public LoadInfo withFileLoad(long fileLoad) {
        this.fileLoad = fileLoad;
        return this;
    }

    public long getSizeOfFiles() {
        return sizeOfFiles;
    }

    public void setSizeOfFiles(long sizeOfFiles) {
        this.sizeOfFiles = sizeOfFiles;
    }

    public LoadInfo withSizeOfFiles(long sizeOfFiles) {
        this.sizeOfFiles = sizeOfFiles;
        return this;
    }

    public long getReadLoad() {
        return readLoad;
    }

    public void setReadLoad(long readLoad) {
        this.readLoad = readLoad;
    }

    public LoadInfo withReadLoad(long readLoad) {
        this.readLoad = readLoad;
        return this;
    }

    public long getWriteLoad() {
        return writeLoad;
    }

    public void setWriteLoad(long writeLoad) {
        this.writeLoad = writeLoad;
    }

    public LoadInfo withWriteLoad(long writeLoad) {
        this.writeLoad = writeLoad;
        return this;
    }

    public long getNumberOfMiss() {
        return numberOfMiss;
    }

    public void setNumberOfMiss(long numberOfMiss) {
        this.numberOfMiss = numberOfMiss;
    }

    public LoadInfo withNumberOfMiss(long numberOfMiss) {
        this.numberOfMiss = numberOfMiss;
        return this;
    }

    public long getNumberOfLockConflicts() {
        return numberOfLockConflicts;
    }

    public void setNumberOfLockConflicts(long numberOfLockConflicts) {
        this.numberOfLockConflicts = numberOfLockConflicts;
    }

    public LoadInfo withNumberOfLockConflicts(long numberOfLockConflicts) {
        this.numberOfLockConflicts = numberOfLockConflicts;
        return this;
    }

    public long getNumberOfHits() {
        return numberOfHits;
    }

    public void setNumberOfHits(long numberOfHits) {
        this.numberOfHits = numberOfHits;
    }

    public LoadInfo withNumberOfHits(long numberOfHits) {
        this.numberOfHits = numberOfHits;
        return this;
    }

    public List<FileBucket> getBucketInfoList() {
        return bucketInfoList;
    }

    public void setBucketInfoList(List<FileBucket> bucketInfoList) {
        this.bucketInfoList = bucketInfoList;
    }

    public LoadInfo withBucketInfoList(List<FileBucket> bucketInfoList) {
        this.bucketInfoList = bucketInfoList;
        return this;
    }

    public boolean isConsolidated() {
        return consolidated;
    }

    public void setConsolidated(boolean consolidated) {
        this.consolidated = consolidated;
    }

    public boolean isLoadBalancing() {
        return loadBalancing;
    }

    public void setLoadBalancing(boolean loadBalancing) {
        this.loadBalancing = loadBalancing;
    }

    public int getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(int serialNumber) {
        this.serialNumber = serialNumber;
    }

    public long getLoad() {
        return readLoad + writeLoad;
    }

    public int getLoadLevel(long lower, long upper) {
        long load = getLoad();
        if (load < lower) {
            if (load < 0.3 * lower) {
                return LEVEL_VERY_LIGHT;
            } else if (load < 0.6 * lower) {
                return LEVEL_MEDIAN_LIGHT;
            } else {
                return LEVEL_LIGHT;
            }
        } else if (load < upper) {
            return LEVEL_NORMAL;
        } else {
            if (load < upper + 0.3 * lower) {
                return LEVEL_HEAVY;
            } else if (load < upper + 0.6 * lower) {
                return LEVEL_MEDIAN_HEAVY;
            } else {
                return LEVEL_VERY_HEAVY;
            }
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("reportTime", reportTime).append("nodeId", nodeId).append("fileLoad", fileLoad).append("sizeOfFiles", sizeOfFiles).append("readLoad", readLoad).append("writeLoad", writeLoad).append("numberOfMiss", numberOfMiss).append("numberOfLockConflicts", numberOfLockConflicts).append("numberOfHits", numberOfHits).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(reportTime).append(nodeId).append(numberOfMiss).append(sizeOfFiles).append(fileLoad).append(readLoad).append(numberOfHits).append(numberOfLockConflicts).append(writeLoad).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof LoadInfo) == false) {
            return false;
        }
        LoadInfo rhs = ((LoadInfo) other);
        return new EqualsBuilder().append(reportTime, rhs.reportTime).append(nodeId, rhs.nodeId).append(numberOfMiss, rhs.numberOfMiss).append(sizeOfFiles, rhs.sizeOfFiles).append(fileLoad, rhs.fileLoad).append(readLoad, rhs.readLoad).append(numberOfHits, rhs.numberOfHits).append(numberOfLockConflicts, rhs.numberOfLockConflicts).append(writeLoad, rhs.writeLoad).isEquals();
    }

}