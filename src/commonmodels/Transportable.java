package commonmodels;

import ceph.Cluster;
import ceph.ClusterMap;
import ceph.PlacementGroup;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.BucketNode;
import filemanagement.FileBucket;
import loadmanagement.LoadInfo;
import ring.VirtualNode;
import statmanagement.StatInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BucketNode.class, name = "BucketNode"),
        @JsonSubTypes.Type(value = ClusterMap.class, name = "ClusterMap"),
        @JsonSubTypes.Type(value = Cluster.class, name = "Cluster"),
        @JsonSubTypes.Type(value = elastic.LookupTable.class, name = "elastic.LookupTable"),
        @JsonSubTypes.Type(value = FileBucket.class, name = "FileBucket"),
        @JsonSubTypes.Type(value = LoadInfo.class, name = "LoadInfo"),
        @JsonSubTypes.Type(value = PhysicalNode.class, name = "PhysicalNode"),
        @JsonSubTypes.Type(value = PlacementGroup.class, name = "PlacementGroup"),
        @JsonSubTypes.Type(value = Request.class, name = "Request"),
        @JsonSubTypes.Type(value = Response.class, name = "Response"),
        @JsonSubTypes.Type(value = ring.LookupTable.class, name = "ring.LookupTable"),
        @JsonSubTypes.Type(value = StatInfo.class, name = "StatInfo"),
        @JsonSubTypes.Type(value = VirtualNode.class, name = "VirtualNode"),
        @JsonSubTypes.Type(value = TransportableString.class, name = "TransportableString")
})
public class Transportable {
}
