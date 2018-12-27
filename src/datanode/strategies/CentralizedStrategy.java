package datanode.strategies;

import commonmodels.DataNode;

public class CentralizedStrategy extends MembershipStrategy {

    public CentralizedStrategy(DataNode dataNode) {
        super(dataNode);
    }

    @Override
    public String getMembersStatus() {
        return dataNode.execute(dataNode.prepareListPhysicalNodesCommand());
    }

}
