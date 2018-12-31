package elastic;

import commonmodels.Indexable;
import filemanagement.LocalFileManager;
import util.MathX;

public class ElasticReadWriteAlgorithm {
    public Indexable read(LookupTable table, String filename) {
        int hash = MathX.positiveHash(filename.hashCode()) % table.getTable().length;
        return table.getTable()[hash];
    }

    public Indexable write(LookupTable table, String filename) {
        int hash = MathX.positiveHash(filename.hashCode()) % table.getTable().length;

        LocalFileManager.getInstance().write(hash);

        return table.getTable()[hash];
    }
}
