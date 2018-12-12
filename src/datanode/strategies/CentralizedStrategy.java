package datanode.strategies;

import commonmodels.DataNode;

public class CentralizedStrategy extends MembershipStrategy {

    public CentralizedStrategy(DataNode dataNode) {
        super(dataNode);
    }

    @Override
    public void onNodeStarted() {

    }

    @Override
    public void onNodeStopped() {

    }

    @Override
    public String getMembersStatus() {
        return null;
    }

}
