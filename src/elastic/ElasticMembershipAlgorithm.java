package elastic;

import commonmodels.PhysicalNode;
import filemanagement.LocalFileManager;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

public class ElasticMembershipAlgorithm {

    public void initialize(LookupTable table) {
        SimpleLog.i("Initializing table...");

        Config config = Config.getInstance();

        int numberOfHashSlots = config.getNumberOfHashSlots();
        String[] nodes = config.getNodes();
        int startPort = config.getStartPort();
        int portRange = config.getPortRange();
        int numberOfActiveNodes = config.getInitNumberOfActiveNodes();
        int numberOfReplicas = config.getNumberOfReplicas();

        List<PhysicalNode> pnodes = new ArrayList<>(); // use for reference when generate table
        int counter = 0;
        outerloop:
        for (int port = startPort; port < startPort + portRange; port++) {
            for (String ip : nodes) {
                PhysicalNode node = new PhysicalNode();
                node.setAddress(ip);
                node.setPort(port);
                table.getPhysicalNodeMap().put(node.getId(), node);
                pnodes.add(node);

                if (++counter >= numberOfActiveNodes)
                    break outerloop;
            }
        }
        PhysicalNode[] pnodesArray = new PhysicalNode[pnodes.size()];
        pnodes.toArray(pnodesArray);
        MathX.shuffle(pnodesArray);
        pnodes = Arrays.asList(pnodesArray);

        // generate table
        Integer[] array = new Integer[numberOfHashSlots];
        table.createTable(numberOfHashSlots);
        for (int i = 0; i < numberOfHashSlots; i++) {
            array[i] = i;
        }

        // randomly map bucket to physical nodes
        MathX.shuffle(array);
        int iterator = 0;
        for (int i : array) {
            int count = 0;
            while (count++ < numberOfReplicas) {
                BucketNode bucketNode = table.getTable()[i];
                PhysicalNode physicalNode = pnodes.get(iterator++ % numberOfActiveNodes);
                bucketNode.getPhysicalNodes().add(physicalNode.getId());
                physicalNode.getVirtualNodes().add(bucketNode);
            }
        }

        SimpleLog.i("Allocating files...");
        LocalFileManager.getInstance().generateFileBuckets(numberOfHashSlots);
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

        int[] bucketPool = generateSpareBuckets(table);
        for (int bucket : bucketPool) {
            BucketNode bucketNode = table.getTable()[bucket];
            node.getVirtualNodes().add(bucketNode);

            table.transferBucket(bucketNode, node);
        }

        SimpleLog.i("Physical node added...");
    }

    public void addPhysicalNode(LookupTable table, PhysicalNode node, int[] buckets) {
        if (buckets == null || buckets.length == 0) {
            addPhysicalNode(table, node);
            return;
        }

        if (table.getPhysicalNodeMap().containsKey(node.getId())) {
            SimpleLog.i(node.getId() + " already exists. Try a different ip:port");
            return;
        }

        SimpleLog.i("Adding new physical node: " + node.toString() + "...");
        table.getPhysicalNodeMap().put(node.getId(), node);

        for (int bucket: buckets) {
            BucketNode bucketNode = table.getTable()[bucket];
            node.getVirtualNodes().add(bucketNode);

            table.transferBucket(bucketNode, node);
        }

        SimpleLog.i("Physical node added...");
    }

    public int[] generateSpareBuckets(LookupTable table) {
        int numberOfHashSlots = Config.getInstance().getNumberOfHashSlots();
        Queue<Integer> bucketPool = MathX.nonrepeatRandom(numberOfHashSlots, numberOfHashSlots / table.getPhysicalNodeMap().size());

        return bucketPool.stream().mapToInt(Integer::intValue).toArray();
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
