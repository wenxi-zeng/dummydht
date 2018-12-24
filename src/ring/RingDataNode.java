package ring;

import commonmodels.DataNode;
import util.ResourcesLoader;
import util.StringHelper;

import java.util.ResourceBundle;

import static util.Config.CONFIG_RING;

public class RingDataNode extends DataNode {

    @Override
    public void createTerminal() {
        terminal = new RingTerminal();
    }

    @Override
    public ResourceBundle loadConfig() {
        return ResourcesLoader.getBundle(CONFIG_RING);
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

    @Override
    public String prepareAddNodeCommand() {
        String buckets = StringHelper.join(LookupTable.getInstance().getSpareBuckets());
        return String.format(RingCommand.ADDNODE.getParameterizedString(), ip, port, buckets);
    }

    @Override
    public String prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return String.format(RingCommand.REMOVENODE.getParameterizedString(), nodeIp, nodePort);
    }
}