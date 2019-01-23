package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import filemanagement.DummyFile;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import util.Config;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

import static util.Config.STATUS_ACTIVE;

public class CephReadWriteAlgorithm {
    public List<PhysicalNode> lookup(ClusterMap clusterMap, String filename) {
        String pgid = clusterMap.getPlacementGroupId(filename);
        int r = 0;

        List<PhysicalNode> pnodes = new ArrayList<>();
        while (pnodes.size() < Config.getInstance().getNumberOfReplicas()) {
            Clusterable node = clusterMap.rush(pgid, r++);

            if (node != null && node.getStatus().equals(STATUS_ACTIVE)) {
                pnodes.add((PhysicalNode) node);
            }
        }

        return pnodes;
    }

    public FileBucket writeAndReplicate(ClusterMap map, DummyFile file) {
        FileBucket fileBucket = writeOnly(map, file);

        if (fileBucket != null && !fileBucket.isLocked() && map.getReadWriteCallBack() != null) {
            List<PhysicalNode> replicas = lookup(map, file.getName());
            map.getReadWriteCallBack().onFileWritten(file.toAttachment(), replicas);
        }

        return fileBucket;
    }

    public FileBucket writeOnly(ClusterMap map, DummyFile file) {
        return LocalFileManager.getInstance().write(file,
                (String str) -> MathX.positiveHash(str.hashCode()) % Config.getInstance().getNumberOfPlacementGroups());
    }
}
