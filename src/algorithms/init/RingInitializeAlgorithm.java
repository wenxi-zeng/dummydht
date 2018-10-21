package algorithms.init;

import models.LookupTable;
import models.PhysicalNode;
import models.VirtualNode;
import util.MathX;

import java.util.Queue;
import java.util.ResourceBundle;

import static util.Config.*;

public class RingInitializeAlgorithm implements TableInitializeAlgorithm {
    @Override
    public void initialize(LookupTable table) {
        ResourceBundle rb = ResourceBundle.getBundle(CONFIG_RING);

        int hashSlots = Integer.valueOf(rb.getString(PROPERTY_HASH_SLOTS));
        String startIp = rb.getString(PROPERTY_START_IP);
        int ipRange = Integer.valueOf(rb.getString(PROPERTY_IP_RANGE));
        int startPort = Integer.valueOf(rb.getString(PROPERTY_START_PORT));
        int portRange = Integer.valueOf(rb.getString(PROPERTY_PORT_RANGE));
        NUMBER_OF_REPLICAS = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_REPLICAS));
        int numberOfPhysicalNodes = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_PHYSICAL_NODES));
        int virtualPhysicalRatio = Integer.valueOf(rb.getString(PROPERTY_VIRTUAL_PHYSICAL_RATIO));

        int lastDot = startIp.lastIndexOf(".");
        String ipPrefix = startIp.substring(0, lastDot);
        int intStartIp = Integer.valueOf(startIp.substring(startIp.lastIndexOf(".") + 1, startIp.length() - 1));

        int totalNodes = numberOfPhysicalNodes * virtualPhysicalRatio;
        Queue<Integer> ipPool = MathX.nonrepeatRandom(ipRange, numberOfPhysicalNodes);
        Queue<Integer> portPool = MathX.nonrepeatRandom(portRange, numberOfPhysicalNodes);
        Queue<Integer> hashPool = MathX.nonrepeatRandom(hashSlots, totalNodes);

        while (!ipPool.isEmpty()){
            Integer ip = ipPool.poll();
            Integer port = portPool.poll();
            assert port != null;

            PhysicalNode node = new PhysicalNode();
            node.setAddress(ipPrefix + (intStartIp + ip));
            node.setPort(startPort + port);
            node.setId("P" + port);
            table.getPhysicalNodeMap().put(node.getId(), node);

            for (int i = 0; i < virtualPhysicalRatio; i++) {
                Integer hash = hashPool.poll();
                assert hash != null;

                VirtualNode vnode = new VirtualNode(hash, node.getId());
                node.getVirtualNodes().add(vnode);
                table.getTable().add(vnode);
            }
        }
    }
}
