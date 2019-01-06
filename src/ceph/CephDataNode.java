package ceph;

import commands.CephCommand;
import commonmodels.DataNode;
import commonmodels.LoadBalancingCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

public class CephDataNode extends DataNode {

    @Override
    public void createTerminal() {
        terminal = new CephTerminal();
    }

    @Override
    public Object getTable() {
        return ClusterMap.getInstance();
    }

    @Override
    public String updateTable(Object o) {
        return ClusterMap.getInstance().updateTable(o);
    }

    @Override
    public List<PhysicalNode> getPhysicalNodes() {
        return new ArrayList<>(
                ClusterMap.getInstance().getPhysicalNodeMap().values()
        );
    }

    @Override
    public Request prepareListPhysicalNodesCommand() {
        return new Request()
                .withHeader(CephCommand.LISTPHYSICALNODES.name());
    }

    @Override
    public Request prepareAddNodeCommand() {
        return prepareAddNodeCommand(ip, port);
    }

    @Override
    public Request prepareAddNodeCommand(String nodeIp, int nodePort) {
        return null;
    }

    @Override
    public Request prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return new Request()
                .withHeader(CephCommand.REMOVENODE.name())
                .withAttachments(nodeIp + ":" + nodePort);
    }

    @Override
    public Request prepareLoadBalancingCommand(String... addresses) {
        int weight = MathX.nextInt((int)ClusterMap.getInstance().getRoot().getWeight());
        return new Request().withHeader(CephCommand.CHANGEWEIGHT.name())
                .withAttachment(addresses[0] + " " + weight);
    }

    @Override
    public void setLoadBalancingCallBack(LoadBalancingCallBack callBack) {
        ClusterMap.getInstance().setLoadBalancingCallBack(callBack);
    }

}
