package commonmodels;

public interface Clusterable {

    String getId();

    void setId(String id);

    float getWeight();

    Clusterable[] getSubClusters();

    void setSubClusters(Clusterable[] subClusters);

    String getStatus();

    void setWeight(float weight);

    void setStatus(String status);

}
