package algorithms.readwrite;

import models.Indexable;
import models.LookupTable;

public interface ReadWriteAlgorithm {

    Indexable read(LookupTable table, String filename);

    Indexable write(LookupTable table, String filename);

}
