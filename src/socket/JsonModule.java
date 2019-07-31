package socket;

import ceph.Cluster;
import ceph.ClusterMap;
import ceph.PlacementGroup;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.module.SimpleModule;
import commonmodels.BinarySearchList;
import commonmodels.Clusterable;
import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.BucketNode;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import ring.VirtualNode;
import statmanagement.StatInfo;

import java.util.HashMap;
import java.util.List;

abstract class PhysicalNodeMixin {
    @JsonCreator
    PhysicalNodeMixin(
            @JsonProperty("address") String address,
            @JsonProperty("port") int port,
            @JsonProperty("status") String status,
            @JsonProperty("virtualNodes") List<Indexable> virtualNodes,
            @JsonProperty("weight") float weight
    ) { }
    @JsonProperty("address") abstract String getAddress();
    @JsonProperty("port") abstract int getPort();
    @JsonProperty("status") abstract String getStatus();
    @JsonProperty("virtualNodes") abstract List<Indexable> getVirtualNodes();
    @JsonProperty("weight") abstract float getWeight();
}

abstract class RequestMixin {
    @JsonCreator
    RequestMixin(
            @JsonProperty("header") String header,
            @JsonProperty("sender") String sender,
            @JsonProperty("receiver") String receiver,
            @JsonProperty("followup") String followup,
            @JsonProperty("attachment") String attachment,
            @JsonProperty("epoch") long epoch,
            @JsonProperty("token") String token,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("largeAttachment") Object largeAttachment
    ) { }
    @JsonProperty("header") abstract String getHeader();
    @JsonProperty("sender") abstract String getSender();
    @JsonProperty("receiver") abstract String getReceiver();
    @JsonProperty("followup") abstract String getFollowup();
    @JsonProperty("attachment") abstract String getAttachment();
    @JsonProperty("epoch") abstract long getEpoch();
    @JsonProperty("token") abstract String getToken();
    @JsonProperty("timestamp") abstract long getTimestamp();
    @JsonProperty("largeAttachment") abstract Object getLargeAttachment();
}

abstract class ResponseMixin {
    @JsonCreator
    ResponseMixin(
            @JsonProperty("header") String header,
            @JsonProperty("status") short status,
            @JsonProperty("message") String message,
            @JsonProperty("token") String token,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("attachment") Object attachment
    ) { }
    @JsonProperty("header") abstract String getHeader();
    @JsonProperty("status") abstract short getStatus();
    @JsonProperty("message") abstract String getMessage();
    @JsonProperty("token") abstract String getToken();
    @JsonProperty("timestamp") abstract long getTimestamp();
    @JsonProperty("attachment") abstract Object getAttachment();
}

abstract class PlacementGroupMixin {
    @JsonCreator
    PlacementGroupMixin(
            @JsonProperty("id") String id,
            @JsonProperty("index") int index,
            @JsonProperty("hash") int hash
    ) { }
    @JsonProperty("id") abstract String getId();
    @JsonProperty("index") abstract int getIndex();
    @JsonProperty("hash") abstract int getHash();
}

abstract class BucketNodeMixin {
    @JsonCreator
    BucketNodeMixin(
            @JsonProperty("hash") int hash,
            @JsonProperty("physicalNodes") List<String> physicalNodes
    ) { }
    @JsonProperty("hash") abstract int getHash();
    @JsonProperty("index") abstract List<String> getPhysicalNodes();
}

abstract class VirtualNodeMixin {
    @JsonCreator
    VirtualNodeMixin(
            @JsonProperty("hash") int hash,
            @JsonProperty("index") int index,
            @JsonProperty("physicalNodeId") String physicalNodeId
    ) { }
    @JsonProperty("hash") abstract int getHash();
    @JsonProperty("index") abstract int getIndex();
    @JsonProperty("physicalNodeId") abstract String getPhysicalNodeId();
}

abstract class ClusterMixin {
    @JsonCreator
    ClusterMixin(
            @JsonProperty("id") String id,
            @JsonProperty("weight") float weight,
            @JsonProperty("subCluster") Clusterable[] subCluster,
            @JsonProperty("status") String status
    ) { }
    @JsonProperty("id") abstract String getId();
    @JsonProperty("weight") abstract float getWeight();
    @JsonProperty("subCluster") abstract Clusterable[] getSubClusters();
    @JsonProperty("status") abstract String getStatus();
}

abstract class LoadInfoMixin {
    @JsonCreator
    LoadInfoMixin(
            @JsonProperty("reportTime") long reportTime,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("fileLoad") long fileLoad,
            @JsonProperty("sizeOfFiles") long sizeOfFiles,
            @JsonProperty("readLoad") long readLoad,
            @JsonProperty("writeLoad") long writeLoad,
            @JsonProperty("numberOfMiss") long numberOfMiss,
            @JsonProperty("numberOfLockConflicts") long numberOfLockConflicts,
            @JsonProperty("numberOfHits") long numberOfHits,
            @JsonProperty("bucketInfoList") List<FileBucket> bucketInfoList
    ) { }
    @JsonProperty("reportTime") abstract long getReportTime();
    @JsonProperty("nodeId") abstract String getNodeId();
    @JsonProperty("fileLoad") abstract long getFileLoad();
    @JsonProperty("sizeOfFiles") abstract long getSizeOfFiles();
    @JsonProperty("readLoad") abstract long getReadLoad();
    @JsonProperty("writeLoad") abstract long getWriteLoad();
    @JsonProperty("numberOfMiss") abstract long getNumberOfMiss();
    @JsonProperty("numberOfLockConflicts") abstract long getNumberOfLockConflicts();
    @JsonProperty("numberOfHits") abstract long getNumberOfHits();
    @JsonProperty("bucketInfoList") abstract List<FileBucket> getBucketInfoList();
}

abstract class FileBucketMixin {
    @JsonCreator
    FileBucketMixin(
            @JsonProperty("key") int key,
            @JsonProperty("size") long size,
            @JsonProperty("numberOfFiles") long numberOfFiles,
            @JsonProperty("sizeOfReads") long sizeOfReads,
            @JsonProperty("sizeOfWrites") long sizeOfWrites,
            @JsonProperty("numberOfReads") long numberOfReads,
            @JsonProperty("numberOfWrites") long numberOfWrites,
            @JsonProperty("numberOfLockConflicts") long numberOfLockConflicts,
            @JsonProperty("locked") boolean locked
    ) { }
    @JsonProperty("key") abstract int getKey();
    @JsonProperty("size") abstract long getSize();
    @JsonProperty("numberOfFiles") abstract long getNumberOfFiles();
    @JsonProperty("sizeOfReads") abstract long getSizeOfReads();
    @JsonProperty("sizeOfWrites") abstract long getSizeOfWrites();
    @JsonProperty("numberOfReads") abstract long getNumberOfReads();
    @JsonProperty("numberOfWrites") abstract long getNumberOfWrites();
    @JsonProperty("numberOfLockConflicts") abstract long getNumberOfLockConflicts();
    @JsonProperty("locked") abstract boolean isLocked();
}

abstract class StatInfoMixin {
    @JsonCreator
    StatInfoMixin(
            @JsonProperty("startTime") long startTime,
            @JsonProperty("endTime") long endTime,
            @JsonProperty("header") String header,
            @JsonProperty("token") String token,
            @JsonProperty("elapsed") long elapsed,
            @JsonProperty("type") String type,
            @JsonProperty("size") long size
    ) { }
    @JsonProperty("startTime") abstract long getStartTime();
    @JsonProperty("endTime") abstract long getEndTime();
    @JsonProperty("header") abstract String getHeader();
    @JsonProperty("token") abstract String getToken();
    @JsonProperty("elapsed") abstract long getElapsed();
    @JsonProperty("type") abstract String getType();
    @JsonProperty("size") abstract long getSize();
}

abstract class RingLookupTableMixin {
    @JsonCreator
    RingLookupTableMixin(
            @JsonProperty("epoch") long epoch,
            @JsonProperty("table") BinarySearchList table,
            @JsonProperty("physicalNodeMap") HashMap<String, PhysicalNode> physicalNodeMap
    ) { }
    @JsonProperty("epoch") abstract long getEpoch();
    @JsonProperty("table") abstract BinarySearchList getTable();
    @JsonProperty("physicalNodeMap") abstract HashMap<String, PhysicalNode> getPhysicalNodeMap();
}

abstract class ElasticLookupTableMixin {
    @JsonCreator
    ElasticLookupTableMixin(
            @JsonProperty("epoch") long epoch,
            @JsonProperty("table") BucketNode[] table,
            @JsonProperty("physicalNodeMap") HashMap<String, PhysicalNode> physicalNodeMap
    ) { }
    @JsonProperty("epoch") abstract long getEpoch();
    @JsonProperty("table") abstract BucketNode[] getTable();
    @JsonProperty("physicalNodeMap") abstract HashMap<String, PhysicalNode> getPhysicalNodeMap();
}

abstract class ClusterMapMixin {
    @JsonCreator
    ClusterMapMixin(
            @JsonProperty("epoch") long epoch,
            @JsonProperty("root") Clusterable root,
            @JsonProperty("physicalNodeMap") HashMap<String, PhysicalNode> physicalNodeMap
    ) { }
    @JsonProperty("epoch") abstract long getEpoch();
    @JsonProperty("root") abstract Clusterable getRoot();
    @JsonProperty("physicalNodeMap") abstract HashMap<String, PhysicalNode> getPhysicalNodeMap();
}

public class JsonModule extends SimpleModule {

    private static final long serialVersionUID = 6134836523275023419L;

    public JsonModule() {
        super("JsonModule");
    }

    @Override
    public void setupModule(SetupContext context) {
        context.setMixInAnnotations(BucketNode.class, BucketNodeMixin.class);
        context.setMixInAnnotations(ClusterMap.class, ClusterMapMixin.class);
        context.setMixInAnnotations(Cluster.class, ClusterMixin.class);
        context.setMixInAnnotations(elastic.LookupTable.class, ElasticLookupTableMixin.class);
        context.setMixInAnnotations(FileBucket.class, FileBucketMixin.class);
        context.setMixInAnnotations(LoadInfo.class, LoadInfoMixin.class);
        context.setMixInAnnotations(PhysicalNode.class, PhysicalNodeMixin.class);
        context.setMixInAnnotations(PlacementGroup.class, PlacementGroupMixin.class);
        context.setMixInAnnotations(Request.class, RequestMixin.class);
        context.setMixInAnnotations(Response.class, ResponseMixin.class);
        context.setMixInAnnotations(ring.LookupTable.class, RingLookupTableMixin.class);
        context.setMixInAnnotations(StatInfo.class, StatInfoMixin.class);
        context.setMixInAnnotations(VirtualNode.class, VirtualNodeMixin.class);
    }

}
