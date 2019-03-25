package loadmanagement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import commonmodels.Queueable;
import filemanagement.FileBucket;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "report_time",
        "node_id",
        "file_load",
        "size_of_files",
        "read_load",
        "write_load",
        "number_of_miss",
        "number_of_lock_conflicts",
        "number_of_hits"
})
public class LoadInfo implements Serializable, Queueable
{
    @JsonProperty("report_time")
    private long reportTime;
    @JsonProperty("node_id")
    private String nodeId;
    @JsonProperty("file_load")
    private long fileLoad;
    @JsonProperty("size_of_files")
    private long sizeOfFiles;
    @JsonProperty("read_load")
    private long readLoad;
    @JsonProperty("write_load")
    private long writeLoad;
    @JsonProperty("number_of_miss")
    private long numberOfMiss;
    @JsonProperty("number_of_lock_conflicts")
    private long numberOfLockConflicts;
    @JsonProperty("number_of_hits")
    private long numberOfHits;
    private final static long serialVersionUID = -607052110101864782L;

    private List<FileBucket> bucketInfoList = new ArrayList<>();

    private transient boolean consolidated = false;

    private transient boolean loadBalancing = false;

    /**
     * No args constructor for use in serialization
     *
     */
    public LoadInfo() {
    }

    /**
     *
     * @param reportTime
     * @param nodeId
     * @param numberOfMiss
     * @param sizeOfFiles
     * @param fileLoad
     * @param readLoad
     * @param numberOfHits
     * @param numberOfLockConflicts
     * @param writeLoad
     */
    public LoadInfo(long reportTime, String nodeId, long fileLoad, long sizeOfFiles, long readLoad, long writeLoad, long numberOfMiss, long numberOfLockConflicts, long numberOfHits) {
        super();
        this.reportTime = reportTime;
        this.nodeId = nodeId;
        this.fileLoad = fileLoad;
        this.sizeOfFiles = sizeOfFiles;
        this.readLoad = readLoad;
        this.writeLoad = writeLoad;
        this.numberOfMiss = numberOfMiss;
        this.numberOfLockConflicts = numberOfLockConflicts;
        this.numberOfHits = numberOfHits;
    }

    @JsonProperty("report_time")
    public long getReportTime() {
        return reportTime;
    }

    @JsonProperty("report_time")
    public void setReportTime(long reportTime) {
        this.reportTime = reportTime;
    }

    public LoadInfo withReportTime(long reportTime) {
        this.reportTime = reportTime;
        return this;
    }

    @JsonProperty("node_id")
    public String getNodeId() {
        return nodeId;
    }

    @JsonProperty("node_id")
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public LoadInfo withNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    @JsonProperty("file_load")
    public long getFileLoad() {
        return fileLoad;
    }

    @JsonProperty("file_load")
    public void setFileLoad(long fileLoad) {
        this.fileLoad = fileLoad;
    }

    public LoadInfo withFileLoad(long fileLoad) {
        this.fileLoad = fileLoad;
        return this;
    }

    @JsonProperty("size_of_files")
    public long getSizeOfFiles() {
        return sizeOfFiles;
    }

    @JsonProperty("size_of_files")
    public void setSizeOfFiles(long sizeOfFiles) {
        this.sizeOfFiles = sizeOfFiles;
    }

    public LoadInfo withSizeOfFiles(long sizeOfFiles) {
        this.sizeOfFiles = sizeOfFiles;
        return this;
    }

    @JsonProperty("read_load")
    public long getReadLoad() {
        return readLoad;
    }

    @JsonProperty("read_load")
    public void setReadLoad(long readLoad) {
        this.readLoad = readLoad;
    }

    public LoadInfo withReadLoad(long readLoad) {
        this.readLoad = readLoad;
        return this;
    }

    @JsonProperty("write_load")
    public long getWriteLoad() {
        return writeLoad;
    }

    @JsonProperty("write_load")
    public void setWriteLoad(long writeLoad) {
        this.writeLoad = writeLoad;
    }

    public LoadInfo withWriteLoad(long writeLoad) {
        this.writeLoad = writeLoad;
        return this;
    }

    @JsonProperty("number_of_miss")
    public long getNumberOfMiss() {
        return numberOfMiss;
    }

    @JsonProperty("number_of_miss")
    public void setNumberOfMiss(long numberOfMiss) {
        this.numberOfMiss = numberOfMiss;
    }

    public LoadInfo withNumberOfMiss(long numberOfMiss) {
        this.numberOfMiss = numberOfMiss;
        return this;
    }

    @JsonProperty("number_of_lock_conflicts")
    public long getNumberOfLockConflicts() {
        return numberOfLockConflicts;
    }

    @JsonProperty("number_of_lock_conflicts")
    public void setNumberOfLockConflicts(long numberOfLockConflicts) {
        this.numberOfLockConflicts = numberOfLockConflicts;
    }

    public LoadInfo withNumberOfLockConflicts(long numberOfLockConflicts) {
        this.numberOfLockConflicts = numberOfLockConflicts;
        return this;
    }

    @JsonProperty("number_of_hits")
    public long getNumberOfHits() {
        return numberOfHits;
    }

    @JsonProperty("number_of_hits")
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