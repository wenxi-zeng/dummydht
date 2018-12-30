package elastic;

import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import org.apache.commons.lang3.StringUtils;
import util.ResourcesLoader;

import java.util.ArrayList;
import java.util.List;
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
    public List<PhysicalNode> getPhysicalNodes() {
        return new ArrayList<>(
                LookupTable.getInstance().getPhysicalNodeMap().values()
        );
    }

    @Override
    public String prepareListPhysicalNodesCommand() {
        return ElasticCommand.LISTPHYSICALNODES.getParameterizedString();
    }

    @Override
    public String prepareAddNodeCommand() {
        return prepareAddNodeCommand(ip, port);
    }

    @Override
    public String prepareAddNodeCommand(String nodeIp, int nodePort) {
        String buckets = StringUtils.join(LookupTable.getInstance().getSpareBuckets(), ',');
        return String.format(ElasticCommand.ADDNODE.getParameterizedString(), ip, port, buckets);
    }

    @Override
    public String prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return String.format(ElasticCommand.REMOVENODE.getParameterizedString(), nodeIp, nodePort);
    }

}