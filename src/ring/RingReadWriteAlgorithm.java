package ring;

import commonmodels.Indexable;

import static util.Config.NUMBER_OF_HASH_SLOTS;

public class RingReadWriteAlgorithm {

    public Indexable read(LookupTable table, String filename) {
        int hash = filename.hashCode() % NUMBER_OF_HASH_SLOTS;
        return table.getTable().find(new VirtualNode(hash));
    }

    public Indexable write(LookupTable table, String filename) {
        int hash = filename.hashCode() % NUMBER_OF_HASH_SLOTS;
        return table.getTable().find(new VirtualNode(hash));
    }
}
