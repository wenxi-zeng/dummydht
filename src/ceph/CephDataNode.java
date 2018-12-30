package ceph;

import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import util.ResourcesLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import static util.Config.CONFIG_CEPH;

public class CephDataNode extends DataNode {

    @Override
    public void createTerminal() {
        terminal = new CephTerminal();
    }

    @Override
    public ResourceBundle loadConfig() {
        return ResourcesLoader.getBundle(CONFIG_CEPH);
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
    public String prepareListPhysicalNodesCommand() {
        return CephCommand.LISTPHYSICALNODES.getParameterizedString();
    }

    @Override
    public String prepareAddNodeCommand() {
        return prepareAddNodeCommand(ip, port);
    }

    @Override
    public String prepareAddNodeCommand(String nodeIp, int nodePort) {
        return null;
    }

    @Override
    public String prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return String.format(CephCommand.REMOVENODE.getParameterizedString(), nodeIp, nodePort);
    }

}
