package elastic;

import commonmodels.PhysicalNode;
import filemanagement.LocalFileManager;
import util.MathX;
import util.ResourcesLoader;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.ResourceBundle;

import static util.Config.*;

public class ElasticMembershipAlgorithm {

    public void initialize(LookupTable table) {
        SimpleLog.i("Initializing table...");

        ResourceBundle rb = ResourcesLoader.getBundle(CONFIG_ELASTIC);

        NUMBER_OF_HASH_SLOTS = Integer.valueOf(rb.getString(PROPERTY_HASH_SLOTS));
        DEFAULT_NUMBER_OF_HASH_SLOTS = NUMBER_OF_HASH_SLOTS;
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

        List<PhysicalNode> pnodes = new ArrayList<>(); // use for reference when generate table
        while (!ipPool.isEmpty()){
            Integer ip = ipPool.poll();
            Integer port = portPool.poll();
            assert port != null;

            PhysicalNode node = new PhysicalNode();
            node.setAddress(ipPrefix + (intStartIp + ip));
            node.setPort(startPort + port);
            table.getPhysicalNodeMap().put(node.getId(), node);
            pnodes.add(node);
        }

        // generate table
        int[] array = new int[NUMBER_OF_HASH_SLOTS];
        table.createTable(NUMBER_OF_HASH_SLOTS);
        for (int i = 0; i < NUMBER_OF_HASH_SLOTS; i++) {
            array[i] = i;
        }

        // randomly map bucket to physical nodes
        MathX.shuffle(array);
        int iterator = 0;
        for (int i : array) {
            int count = 0;
            while (count++ < NUMBER_OF_REPLICAS) {
                BucketNode bucketNode = table.getTable()[i];
                PhysicalNode physicalNode = pnodes.get(iterator++ % numberOfPhysicalNodes);
                bucketNode.getPhysicalNodes().add(physicalNode.getId());
                physicalNode.getVirtualNodes().add(bucketNode);
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

        Queue<Integer> bucketPool = MathX.nonrepeatRandom(NUMBER_OF_HASH_SLOTS, NUMBER_OF_HASH_SLOTS / table.getPhysicalNodeMap().size());

        while (!bucketPool.isEmpty()) {
            int bucket = bucketPool.poll();

            BucketNode bucketNode = table.getTable()[bucket];
            bucketNode.getPhysicalNodes().add(node.getId());
            node.getVirtualNodes().add(bucketNode);

            table.copyBucket(bucketNode, node);
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

        table.getPhysicalNodeMap().remove(node.getId());
        List<PhysicalNode> physicalNodes = table.getOrderedPhysicalNodeList();

        for (int i = 0; i < pnode.getVirtualNodes().size(); i++) {
            PhysicalNode replica = physicalNodes.get(i % physicalNodes.size());
            BucketNode bucketNode = table.getTable()[pnode.getVirtualNodes().get(i).getHash()];
            bucketNode.getPhysicalNodes().add(replica.getId());
            bucketNode.getPhysicalNodes().remove(pnode.getId());
            replica.getVirtualNodes().add(bucketNode);

            table.copyBucket(bucketNode, replica);
        }

        SimpleLog.i("Physical node removed...");
    }
}
