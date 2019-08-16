package ring;

import commands.RingCommand;
import commonmodels.*;
import commonmodels.transport.Request;
import org.apache.commons.lang3.StringUtils;
import util.MathX;

import java.util.ArrayList;
import java.util.List;

public class RingDataNode extends DataNode {

    public RingDataNode() {
    }

    public RingDataNode(String ip, int port) {
        super(ip, port);
    }

    @Override
    public void createTerminal() {
        terminal = new RingTerminal();
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
    public String createTable(Object o) {
        return LookupTable.getInstance().createTable(o);
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
            return prepareDecreaseLoadCommand(addresses);
        }
        else {
            return prepareIncreaseLoadCommand(addresses);
        }
    }

    @Override
    public Request prepareIncreaseLoadCommand(String... addresses) {
        int[] deltaHash = LookupTable.getInstance().randomIncreaseRange(new PhysicalNode(addresses[0]));
        return new Request().withHeader(RingCommand.INCREASELOAD.name())
                .withAttachments(addresses[0], StringUtils.join(deltaHash, ','));
    }

    @Override
    public Request prepareDecreaseLoadCommand(String... addresses) {
        int[] deltaHash = LookupTable.getInstance().randomDecreaseRange(new PhysicalNode(addresses[0]));
        return new Request().withHeader(RingCommand.DECREASELOAD.name())
                .withAttachments(addresses[0], StringUtils.join(deltaHash, ','));
    }

    @Override
    public void setMembershipCallBack(MembershipCallBack callBack) {
        LookupTable.getInstance().setMembershipCallBack(callBack);
    }

    @Override
    public void setReadWriteCallBack(ReadWriteCallBack callBack) {
        LookupTable.getInstance().setReadWriteCallBack(callBack);
    }

    @Override
    public void initTableDeltaSupplier() {
        LookupTable.getInstance().setDeltaSupplier(this::getTableDelta);
    }
}