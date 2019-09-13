package req.gen;

import commands.CephCommand;
import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import util.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ControlRequestGenerator extends RequestGenerator {

    private List<PhysicalNode> activePhysicalNodes;

    private List<PhysicalNode> inactivePhysicalNodes;

    private DataNode dataNode;

    public ControlRequestGenerator(List<PhysicalNode> physicalNodes, DataNode dataNode) {
        super(physicalNodes.size() - 1);
        this.inactivePhysicalNodes = physicalNodes;
        this.activePhysicalNodes = new ArrayList<>();
        this.dataNode = dataNode;
    }

    @Override
    public Request nextFor(int threadId) throws Exception {
        Request header = headerGenerator.next();
        if (header.getHeader().equals(CephCommand.ADDNODE.name()))
            return nextAdd();
        else if (header.getHeader().equals(CephCommand.REMOVENODE.name()))
            return nextRemove();
        else
            return nextLoadBalancing();
    }

    public Request nextAdd() throws Exception {
        if (inactivePhysicalNodes.size() < 1)
            throw new Exception("No node available for addition anymore.");

        int key = generator.nextInt(inactivePhysicalNodes.size() - 1);
        PhysicalNode node = inactivePhysicalNodes.get(key);
        activePhysicalNodes.add(node);
        inactivePhysicalNodes.remove(node);

        return dataNode.prepareAddNodeCommand(
                node.getAddress(),
                node.getPort());
    }

    public Request nextRemove() throws Exception {
        if (activePhysicalNodes.size() < 1)
            throw new Exception("No node available for removal anymore.");

        int key = generator.nextInt(activePhysicalNodes.size() - 1);
        PhysicalNode node = activePhysicalNodes.get(key);
        inactivePhysicalNodes.add(node);
        activePhysicalNodes.add(node);

        return dataNode.prepareRemoveNodeCommand(
                node.getAddress(),
                node.getPort());
    }

    public Request nextLoadBalancing() throws Exception {
        if (activePhysicalNodes.size() < 2)
            throw new Exception("Not enough active nodes for load balancing");

        int key1 = generator.nextInt(activePhysicalNodes.size() - 1);
        int key2;
        do {
            key2 = generator.nextInt(activePhysicalNodes.size() - 1);
        } while (key1 == key2);

        PhysicalNode node1 = activePhysicalNodes.get(key1);
        PhysicalNode node2 = activePhysicalNodes.get(key2);

        return dataNode.prepareLoadBalancingCommand(
                node1.getAddress() + " " + node1.getPort(),
                node2.getAddress() + " " + node2.getPort());
    }

    @Override
    public Map<Request, Double> loadRequestRatio() {
        double[] ratio = Config.getInstance().getReadWriteRatio();
        Map<Request, Double> map = new HashMap<>();
        map.put(new Request().withHeader(CephCommand.ADDNODE.name()), ratio[Config.RATIO_KEY_READ]);
        map.put(new Request().withHeader(CephCommand.REMOVENODE.name()), ratio[Config.RATIO_KEY_WRITE]);
        map.put(new Request().withHeader(CephCommand.CHANGEWEIGHT.name()), ratio[Config.RATIO_KEY_LOAD_BALANCING]);
        return map;
    }
}