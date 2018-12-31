package elastic;

import commonmodels.DataNode;
import commonmodels.LoadBalancingCallBack;
import commonmodels.PhysicalNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ElasticDataNode extends DataNode {

    @Override
    public void createTerminal() {
        terminal = new ElasticTerminal();
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

    @Override
    public void setLoadBalancingCallBack(LoadBalancingCallBack callBack) {
        LookupTable.getInstance().setLoadBalancingCallBack(callBack);
    }

}