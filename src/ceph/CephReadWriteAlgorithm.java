package ceph;

import commonmodels.Clusterable;
import filemanagement.LocalFileManager;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

import static util.Config.NUMBER_OF_PLACEMENT_GROUPS;
import static util.Config.NUMBER_OF_REPLICAS;
import static util.Config.STATUS_ACTIVE;

public class CephReadWriteAlgorithm {
    public Clusterable read(ClusterMap map, String filename) {
        String pgid = map.getPlacementGroupId(filename);
        int r = 0;

        while (true) {
            Clusterable node = map.rush(pgid, r++);

            if (node != null && node.getStatus().equals(STATUS_ACTIVE))
                return node;
        }
    }

    public List<Clusterable> write(ClusterMap map, String filename) {
        String pgid = map.getPlacementGroupId(filename);
        int r = 0;

        List<Clusterable> replicas = new ArrayList<>();
        while (replicas.size() < NUMBER_OF_REPLICAS) {
            Clusterable node = map.rush(pgid, r++);

            if (node != null && node.getStatus().equals(STATUS_ACTIVE)) {
                replicas.add(node);
            }
        }

        LocalFileManager.getInstance().write(MathX.positiveHash(filename.hashCode()) % NUMBER_OF_PLACEMENT_GROUPS);
        return replicas;
    }
}
