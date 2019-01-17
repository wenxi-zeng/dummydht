package ring;

import commands.RingCommand;
import commonmodels.DataNode;
import commonmodels.LoadBalancingCallBack;
import commonmodels.MembershipCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import org.apache.commons.lang3.StringUtils;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

public class RingDataNode extends DataNode {

    @Override
    public void createTerminal() {
        terminal = new RingTerminal();
    }

    @Override
    public Object getTable() {
        return LookupTable.getInstance();
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
                .withHeader(RingCommand.LISTPHYSICALNODES.name());
    }

    @Override
    public Request prepareAddNodeCommand() {
        return prepareAddNodeCommand(ip, port);
    }

    @Override
    public Request prepareAddNodeCommand(String nodeIp, int nodePort) {
        String buckets = StringUtils.join(LookupTable.getInstance().getSpareBuckets(), ',');
        return new Request()
                .withHeader(RingCommand.ADDNODE.name())
                .withAttachments(nodeIp + ":" + nodePort, buckets);
    }

    @Override
    public Request prepareRemoveNodeCommand(String nodeIp, int nodePort) {
        return new Request()
                .withHeader(RingCommand.REMOVENODE.name())
                .withAttachments(nodeIp + ":" + nodePort);
    }

    @Override
    public Request prepareLoadBalancingCommand(String... addresses) {
        int coin = MathX.nextInt(0, 1);

        if (coin == 0) {
            return new Request().withHeader(RingCommand.DECREASELOAD.name())
                    .withAttachment(addresses[0]);
        }
        else {
            return new Request().withHeader(RingCommand.INCREASELOAD.name())
                    .withAttachment(addresses[0]);
        }
    }

    @Override
    public void setLoadBalancingCallBack(LoadBalancingCallBack callBack) {
        LookupTable.getInstance().setLoadBalancingCallBack(callBack);
    }

    @Override
    public void setMembershipCallBack(MembershipCallBack callBack) {
        LookupTable.getInstance().setMembershipCallBack(callBack);
    }
}