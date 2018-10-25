package commonmodels;

import java.util.List;

public interface Clusterable {

    String getId();

    void setId(String id);

    float getWeight();

    Clusterable[] getSubClusters();

    void setSubClusters(Clusterable[] subClusters);

    List<Clusterable> getLeaves();

    String getStatus();

    void setWeight(float weight);

    void setStatus(String status);

    String toTreeString(String prefix, boolean isTail);
}
