package ring;

import commonmodels.Indexable;
import filemanagement.LocalFileManager;
import util.Config;
import util.MathX;

public class RingReadWriteAlgorithm {

    public Indexable read(LookupTable table, String filename) {
        int hash = MathX.positiveHash(filename.hashCode()) % Config.getInstance().getNumberOfHashSlots();
        Indexable node = table.getTable().find(new VirtualNode(hash));
        return table.getTable().get(node.getIndex());
    }

    public Indexable write(LookupTable table, String filename) {
        int hash = MathX.positiveHash(filename.hashCode()) % Config.getInstance().getNumberOfHashSlots();
        Indexable node = table.getTable().find(new VirtualNode(hash));

        LocalFileManager.getInstance().write(hash);

        return table.getTable().get(node.getIndex());
    }
}
