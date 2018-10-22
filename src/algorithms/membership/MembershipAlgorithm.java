package algorithms.membership;

import models.LookupTable;
import models.PhysicalNode;

public interface MembershipAlgorithm {

    void initialize(LookupTable table);

    void addPhysicalNode(LookupTable table, PhysicalNode node);

    void removePhysicalNode(LookupTable table, PhysicalNode node);

}
