package ceph;

import commonmodels.DataNode;
import util.ResourcesLoader;

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
    public void updateTable(Object o) {
        if (o instanceof ClusterMap) {
            ClusterMap remoteMap = (ClusterMap)o;
            ClusterMap localMap = ClusterMap.getInstance();
            localMap.setRoot(remoteMap.getRoot());
            localMap.setEpoch(remoteMap.getEpoch());
            localMap.setPhysicalNodeMap(remoteMap.getPhysicalNodeMap());
        }
    }

    @Override
    public String prepareAddNodeCommand() {
        return null;
    }

    @Override
    public String prepareRemoveNodeCommand() {
        return String.format(CephCommand.REMOVENODE.getParameterizedString(), ip, port);
    }

}
