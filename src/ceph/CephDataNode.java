package ceph;

import commonmodels.DataNode;
import commonmodels.LoadBalancingCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;

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
        if (o instanceof ClusterMap) {
            ClusterMap remoteMap = (ClusterMap)o;
            ClusterMap localMap = ClusterMap.getInstance();
            localMap.setRoot(remoteMap.getRoot());
            localMap.setEpoch(remoteMap.getEpoch());
            localMap.setPhysicalNodeMap(remoteMap.getPhysicalNodeMap());

            return "Map updated.";
        }
        else {
            return "Invalid map type.";
        }
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
    public void setLoadBalancingCallBack(LoadBalancingCallBack callBack) {
        ClusterMap.getInstance().setLoadBalancingCallBack(callBack);
    }

}
