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
    public void onNodeUp(String cluster, String ip, int port) {
        String command = String.format(CephCommand.ADDNODE.getParameterizedString(), cluster, ip, port);
        terminal.execute(command.split("\\s+"));
    }

    @Override
    public void onNodeDown(String ip, int port) {
        String command = String.format(CephCommand.REMOVENODE.getParameterizedString(), ip, port);
        terminal.execute(command.split("\\s+"));
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

}
