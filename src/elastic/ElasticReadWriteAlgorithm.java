package elastic;

import commonmodels.PhysicalNode;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import util.Config;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

public class ElasticReadWriteAlgorithm {
    public List<PhysicalNode> lookup(LookupTable lookupTable, String filename) {
        List<PhysicalNode> pnodes = new ArrayList<>();

        int hash = MathX.positiveHash(filename.hashCode()) % lookupTable.getTable().length;
        BucketNode node = lookupTable.getTable()[hash];

        for (String pnodeId : node.getPhysicalNodes()) {
            PhysicalNode pnode = lookupTable.getPhysicalNodeMap().get(pnodeId);
            pnodes.add(pnode);
        }

        return pnodes;
    }

    public FileBucket writeAndReplicate(LookupTable table, String file) {
        FileBucket fileBucket = writeOnly(table, file);

        if (fileBucket != null && !fileBucket.isLocked() && table.getReadWriteCallBack() != null) {
            List<PhysicalNode> replicas = lookup(table, file);
            table.getReadWriteCallBack().onFileWritten(file, replicas);
        }

        return fileBucket;
    }

    public FileBucket writeOnly(LookupTable table, String file) {
        String[] temp = file.split(" ");

        String filename = temp[0];
        int hash = MathX.positiveHash(filename.hashCode()) % LookupTable.getInstance().getTable().length;
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
