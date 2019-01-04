package elastic;

import commonmodels.PhysicalNode;
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
}
