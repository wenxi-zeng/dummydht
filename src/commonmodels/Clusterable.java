package commonmodels;

public interface Clusterable {

    String getId();

    float getWeight();

    Clusterable[] getSubClusters();

    String getStatus();

    void setWeight(float weight);

    void setStatus(String status);

}
