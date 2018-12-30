package ring;

import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import org.apache.commons.lang3.StringUtils;
import util.ResourcesLoader;

import java.util.ArrayList;
import java.util.List;
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
    public String updateTable(Object o) {
        if (o instanceof LookupTable) {
            LookupTable remoteTable = (LookupTable)o;
            LookupTable localTable = LookupTable.getInstance();
            localTable.setTable(remoteTable.getTable());
            localTable.setEpoch(remoteTable.getEpoch());
            localTable.setPhysicalNodeMap(remoteTable.getPhysicalNodeMap());

            return "Table updated.";
        }
        else {
            return "Invalid table type.";
        }
    }

    @Override
    public List<PhysicalNode> getPhysicalNodes() {
        return new ArrayList<>(
                LookupTable.getInstance().getPhysicalNodeMap().values()
        );
    }

    @Override
    public String prepareListPhysicalNodesCommand() {
        return RingCommand.LISTPHYSICALNODES.getParameterizedString();
    }

    @Override
    public String prepareAddNodeCommand() {
        return prepareAddNodeCommand(ip, port);
    }

    @Override
    public String prepareAddNodeCommand(String nodeIp, int nodePort) {
        String buckets = StringUtils.join(LookupTable.getInstance().getSpareBuckets(), ',');
        return String.format(RingCommand.ADDNODE.getParameterizedString(), ip, port, buckets);
    }

    @Override
    public String prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return String.format(RingCommand.REMOVENODE.getParameterizedString(), nodeIp, nodePort);
    }
}