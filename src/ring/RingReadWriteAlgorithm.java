package ring;

import commonmodels.Indexable;
import util.MathX;

import static util.Config.NUMBER_OF_HASH_SLOTS;

public class RingReadWriteAlgorithm {

    public Indexable read(LookupTable table, String filename) {
        int hash = MathX.positiveHash(filename.hashCode()) % NUMBER_OF_HASH_SLOTS;
        Indexable node = table.getTable().find(new VirtualNode(hash));
        return table.getTable().get(node.getIndex());
    }

    public Indexable write(LookupTable table, String filename) {
        int hash = MathX.positiveHash(filename.hashCode()) % NUMBER_OF_HASH_SLOTS;
        Indexable node = table.getTable().find(new VirtualNode(hash));
        return table.getTable().get(node.getIndex());
    }
}
