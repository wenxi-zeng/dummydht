package elastic;

import commands.ElasticCommand;
import commonmodels.*;
import commonmodels.transport.Request;
import org.apache.commons.lang3.StringUtils;
import util.Config;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

public class ElasticDataNode extends DataNode {

    public ElasticDataNode() {
    }

    public ElasticDataNode(String ip, int port) {
        super(ip, port);
    }

    @Override
    public void createTerminal() {
        terminal = new ElasticTerminal();
    }

    @Override
    public Object getTable() {
        return LookupTable.getInstance();
    }

    @Override
    public long getEpoch() {
        return LookupTable.getInstance().getEpoch();
    }

    @Override
    public String updateTable(Object o) {
        return LookupTable.getInstance().updateTable(o);
    }

    @Override
    public List<PhysicalNode> getPhysicalNodes() {
        return new ArrayList<>(
                LookupTable.getInstance().getPhysicalNodeMap().values()
        );
    }


    @Override
    public Request prepareListPhysicalNodesCommand() {
        return new Request()
                .withHeader(ElasticCommand.LISTPHYSICALNODES.name());
    }

    @Override
    public Request prepareAddNodeCommand() {
        return prepareAddNodeCommand(ip, port);
    }

    @Override
    public Request prepareAddNodeCommand(String nodeIp, int nodePort) {
        String buckets = StringUtils.join(LookupTable.getInstance().getSpareBuckets(), ',');
        return new Request()
                .withHeader(ElasticCommand.ADDNODE.name())
                .withAttachments(nodeIp + ":" + nodePort, buckets);
    }

    @Override
    public Request prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return new Request()
                .withHeader(ElasticCommand.REMOVENODE.name())
                .withAttachments(nodeIp + ":" + nodePort);
    }

    @Override
    public Request prepareLoadBalancingCommand(String... addresses) {
        int bucket = MathX.nextInt(Config.getInstance().getNumberOfHashSlots());

        return new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                .withAttachment(addresses[0] + " " + addresses[1] + " " + bucket);
    }

    @Override
    public Request prepareIncreaseLoadCommand(String... addresses) {
        int bucket = MathX.nextInt(Config.getInstance().getNumberOfHashSlots());

        return new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                .withAttachment(addresses[1] + " " + addresses[0] + " " + bucket);
    }

    @Override
    public Request prepareDecreaseLoadCommand(String... addresses) {
        int bucket = MathX.nextInt(Config.getInstance().getNumberOfHashSlots());

        return new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                .withAttachment(addresses[0] + " " + addresses[1] + " " + bucket);
    }

    @Override
    public void setLoadBalancingCallBack(LoadBalancingCallBack callBack) {
        LookupTable.getInstance().setLoadBalancingCallBack(callBack);
    }

    @Override
    public void setMembershipCallBack(MembershipCallBack callBack) {
        LookupTable.getInstance().setMembershipCallBack(callBack);
    }

    @Override
    public void setReadWriteCallBack(ReadWriteCallBack callBack) {
        LookupTable.getInstance().setReadWriteCallBack(callBack);
    }

}