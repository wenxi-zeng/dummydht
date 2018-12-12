package datanode.strategies;

import commonmodels.DataNode;

import java.net.URISyntaxException;
import java.net.UnknownHostException;

public abstract class MembershipStrategy {

    protected DataNode dataNode;

    public MembershipStrategy(DataNode dataNode) {
        this.dataNode = dataNode;
    }

    public abstract void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException;

    public abstract void onNodeStopped();

    public abstract String getMembersStatus();
}
