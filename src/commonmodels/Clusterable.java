package commonmodels;

import java.io.Serializable;
import java.util.List;

public interface Clusterable extends Serializable {

    String getId();

    void setId(String id);

    float getWeight();

    Clusterable[] getSubClusters();

    void setSubClusters(Clusterable[] subClusters);

    List<Clusterable> getLeaves();

    String getStatus();

    void setWeight(float weight);

    void setStatus(String status);

    void updateWeight();

    int getNumberOfSubClusters();

    String toTreeString(String prefix, boolean isTail);
}
