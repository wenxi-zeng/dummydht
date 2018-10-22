package algorithms.readwrite;

import models.Indexable;
import models.LookupTable;
import models.VirtualNode;

import static util.Config.NUMBER_OF_HASH_SLOTS;

public class RingReadWriteAlgorithm implements ReadWriteAlgorithm {

    @Override
    public Indexable read(LookupTable table, String filename) {
        int hash = filename.hashCode() % NUMBER_OF_HASH_SLOTS;
        return LookupTable.getInstance().getTable().find(new VirtualNode(hash));
    }

    @Override
    public Indexable write(LookupTable table, String filename) {
        int hash = filename.hashCode() % NUMBER_OF_HASH_SLOTS;
        return LookupTable.getInstance().getTable().find(new VirtualNode(hash));
    }
}
