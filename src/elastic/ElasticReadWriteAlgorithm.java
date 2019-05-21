package elastic;

import commonmodels.PhysicalNode;
import filemanagement.DummyFile;
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

    public FileBucket writeAndReplicate(LookupTable table, DummyFile file) {
        FileBucket fileBucket = writeOnly(table, file);

        if (fileBucket != null && !fileBucket.isLocked() && table.getReadWriteCallBack() != null) {
            List<PhysicalNode> replicas = lookup(table, file.getName());
            table.getReadWriteCallBack().onFileWritten(file.toAttachment(), replicas);
        }

        return fileBucket;
    }

    public FileBucket writeOnly(LookupTable table, DummyFile file) {
        return LocalFileManager.getInstance().write(file,
                (String str) -> MathX.positiveHash(str.hashCode()) % table.getTable().length);
    }
}
