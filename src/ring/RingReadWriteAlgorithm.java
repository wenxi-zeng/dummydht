package ring;

import commonmodels.PhysicalNode;
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
}
