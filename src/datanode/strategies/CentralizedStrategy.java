package datanode.strategies;

import commonmodels.DataNode;
import commonmodels.transport.Response;

public class CentralizedStrategy extends MembershipStrategy {

    public CentralizedStrategy(DataNode dataNode) {
        super(dataNode);
    }

    @Override
    public Response getMembersStatus() {
        return dataNode.execute(dataNode.prepareListPhysicalNodesCommand());
    }

}
