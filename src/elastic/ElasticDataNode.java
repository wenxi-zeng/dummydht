package elastic;

import commonmodels.DataNode;
import util.ResourcesLoader;

import java.util.ResourceBundle;

import static util.Config.CONFIG_ELASTIC;

public class ElasticDataNode extends DataNode {

    @Override
    public void createTerminal() {
        terminal = new ElasticTerminal();
    }

    @Override
    public ResourceBundle loadConfig() {
        return ResourcesLoader.getBundle(CONFIG_ELASTIC);
    }

    @Override
    public void onNodeUp(String cluster, String ip, int port) {
        String command = String.format(ElasticCommand.ADDNODE.getParameterizedString(), ip, port);
        terminal.execute(command.split("\\s+"));
    }

    @Override
    public void onNodeDown(String ip, int port) {
        String command = String.format(ElasticCommand.REMOVENODE.getParameterizedString(), ip, port);
        terminal.execute(command.split("\\s+"));
    }

    @Override
    public Object getTable() {
        return LookupTable.getInstance();
    }

    @Override
    public void updateTable(Object o) {
        if (o instanceof LookupTable) {
            LookupTable remoteTable = (LookupTable)o;
            LookupTable localTable = LookupTable.getInstance();
            localTable.setTable(remoteTable.getTable());
            localTable.setEpoch(remoteTable.getEpoch());
            localTable.setPhysicalNodeMap(remoteTable.getPhysicalNodeMap());
        }
    }

}