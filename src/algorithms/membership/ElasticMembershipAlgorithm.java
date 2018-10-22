package algorithms.membership;

import models.LookupTable;
import models.PhysicalNode;
import util.MathX;
import util.SimpleLog;

import java.util.Queue;
import java.util.ResourceBundle;

import static util.Config.*;

public class ElasticMembershipAlgorithm implements MembershipAlgorithm{
    @Override
    public void initialize(LookupTable table) {
        SimpleLog.i("Initializing table...");

        ResourceBundle rb = ResourceBundle.getBundle(CONFIG_RING);

        NUMBER_OF_HASH_SLOTS = Integer.valueOf(rb.getString(PROPERTY_HASH_SLOTS));
        String startIp = rb.getString(PROPERTY_START_IP);
        int ipRange = Integer.valueOf(rb.getString(PROPERTY_IP_RANGE));
        int startPort = Integer.valueOf(rb.getString(PROPERTY_START_PORT));
        int portRange = Integer.valueOf(rb.getString(PROPERTY_PORT_RANGE));
        NUMBER_OF_REPLICAS = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_REPLICAS));
        int numberOfPhysicalNodes = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_PHYSICAL_NODES));

        int lastDot = startIp.lastIndexOf(".") + 1;
        String ipPrefix = startIp.substring(0, lastDot);
        int intStartIp = Integer.valueOf(startIp.substring(lastDot));

        Queue<Integer> ipPool = MathX.nonrepeatRandom(ipRange, numberOfPhysicalNodes);
        Queue<Integer> portPool = MathX.nonrepeatRandom(portRange, numberOfPhysicalNodes);

        while (!ipPool.isEmpty()){
            Integer ip = ipPool.poll();
            Integer port = portPool.poll();
            assert port != null;

            PhysicalNode node = new PhysicalNode();
            node.setAddress(ipPrefix + (intStartIp + ip));
            node.setPort(startPort + port);
            table.getPhysicalNodeMap().put(node.getId(), node);

            for (int i = 0; i < VIRTUAL_PHYSICAL_RATIO; i++) {
                Integer hash = hashPool.poll();
                assert hash != null;

                VirtualNode vnode = new VirtualNode(hash, node.getId());
                node.getVirtualNodes().add(vnode);
                table.getTable().add(vnode);
            }
        }

        SimpleLog.i("Table initialized...");
    }

    @Override
    public void addPhysicalNode(LookupTable table, PhysicalNode node) {

    }

    @Override
    public void removePhysicalNode(LookupTable table, PhysicalNode node) {

    }
}
