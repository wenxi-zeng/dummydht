package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import filemanagement.LocalFileManager;
import util.MathX;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.ResourceBundle;

import static util.Config.*;

public class RingMembershipAlgorithm {

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
        VIRTUAL_PHYSICAL_RATIO = Integer.valueOf(rb.getString(PROPERTY_VIRTUAL_PHYSICAL_RATIO));

        int lastDot = startIp.lastIndexOf(".") + 1;
        String ipPrefix = startIp.substring(0, lastDot);
        int intStartIp = Integer.valueOf(startIp.substring(lastDot));

        int totalNodes = numberOfPhysicalNodes * VIRTUAL_PHYSICAL_RATIO;
        Queue<Integer> ipPool = MathX.nonrepeatRandom(ipRange, numberOfPhysicalNodes);
        Queue<Integer> portPool = MathX.nonrepeatRandom(portRange, numberOfPhysicalNodes);
        Queue<Integer> hashPool = MathX.nonrepeatRandom(NUMBER_OF_HASH_SLOTS, totalNodes);

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

        SimpleLog.i("Allocating files...");
        LocalFileManager.getInstance().generateFileBuckets(NUMBER_OF_HASH_SLOTS);
        SimpleLog.i("Files allocated...");

        SimpleLog.i("Table initialized...");
    }

    public void addPhysicalNode(LookupTable table, PhysicalNode node) {
        if (table.getPhysicalNodeMap().containsKey(node.getId())) {
            SimpleLog.i(node.getId() + " already exists. Try a different ip:port");
            return;
        }

        SimpleLog.i("Adding new physical node: " + node.toString() + "...");
        table.getPhysicalNodeMap().put(node.getId(), node);

        List<Integer> usedSlots = new ArrayList<>();
        for (int i = 0; i < table.getTable().size(); i++) {
            usedSlots.add(table.getTable().get(i).getHash());
        }
        Queue<Integer> hashPool = MathX.nonrepeatRandom(NUMBER_OF_HASH_SLOTS, VIRTUAL_PHYSICAL_RATIO, usedSlots);

        while (!hashPool.isEmpty()) {
            Integer hash = hashPool.poll();

            VirtualNode vnode = new VirtualNode(hash, node.getId());
            node.getVirtualNodes().add(vnode);
            table.addNode(vnode);
        }

        SimpleLog.i("Physical node added...");
    }

    public void removePhysicalNode(LookupTable table, PhysicalNode node) {
        SimpleLog.i("Remove physical node: " + node.toString() + "...");

        PhysicalNode pnode = table.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        for (Indexable vnode : pnode.getVirtualNodes()) {
            table.removeNode(vnode);
        }

        table.getPhysicalNodeMap().remove(node.getId());

        SimpleLog.i("Physical node removed...");
    }
}
