package ring;

import commonmodels.PhysicalNode;
import commonmodels.transport.Response;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import util.Config;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

public class RingReadWriteAlgorithm {
    public List<PhysicalNode> lookup(LookupTable table, String filename) {
        List<PhysicalNode> pnodes = new ArrayList<>();

        int hash = MathX.positiveHash(filename.hashCode()) % Config.getInstance().getNumberOfHashSlots();
        VirtualNode node = (VirtualNode)table.getTable().findNode(new VirtualNode(hash));

        int i = 0;
        do {
            PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getPhysicalNodeId());
            pnodes.add(pnode);
            node = (VirtualNode)table.getTable().next(node);
        } while (++i < Config.getInstance().getNumberOfReplicas());

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
        int hash = MathX.positiveHash(filename.hashCode()) % Config.getInstance().getNumberOfHashSlots();

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
