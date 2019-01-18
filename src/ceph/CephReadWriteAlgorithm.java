package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
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

    public FileBucket writeAndReplicate(ClusterMap map, String file) {
        FileBucket fileBucket = writeOnly(map, file);

        if (fileBucket != null && !fileBucket.isLocked() && map.getReadWriteCallBack() != null) {
            List<PhysicalNode> replicas = lookup(map, file);
            map.getReadWriteCallBack().onFileWritten(file, replicas);
        }

        return fileBucket;
    }

    public FileBucket writeOnly(ClusterMap map, String file) {
        String[] temp = file.split(" ");

        String filename = temp[0];
        int hash = MathX.positiveHash(filename.hashCode()) % Config.getInstance().getNumberOfPlacementGroups();
        FileBucket fileBucket;
        if (temp.length == 2) {
            long filesize = Long.valueOf(temp[1]);
            fileBucket = LocalFileManager.getInstance().write(hash, filesize);
        }
        else {
            fileBucket = LocalFileManager.getInstance().write(hash);
        }

        return fileBucket;
    }
}
