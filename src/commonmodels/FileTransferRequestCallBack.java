package commonmodels;

import filemanagement.FileBucket;

import java.util.List;

public interface FileTransferRequestCallBack {
    void onTransferring(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode);
    void onReplicating(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode);
    void onTransmitted(List<FileBucket> buckets, PhysicalNode from, PhysicalNode toNode);
}
