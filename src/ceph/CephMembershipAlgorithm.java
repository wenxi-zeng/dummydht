package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import util.MathX;
import util.SimpleLog;

import java.util.Queue;
import java.util.ResourceBundle;

import static util.Config.*;

public class CephMembershipAlgorithm {

    public void initialize(ClusterMap map) {
        SimpleLog.i("Initializing map...");

        ResourceBundle rb = ResourceBundle.getBundle(CONFIG_CEPH);

        NUMBER_OF_PLACEMENT_GROUPS = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_PLACEMENT_GROUPS));
        String startIp = rb.getString(PROPERTY_START_IP);
        int ipRange = Integer.valueOf(rb.getString(PROPERTY_IP_RANGE));
        int startPort = Integer.valueOf(rb.getString(PROPERTY_START_PORT));
        int portRange = Integer.valueOf(rb.getString(PROPERTY_PORT_RANGE));
        NUMBER_OF_REPLICAS = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_REPLICAS));
        int numberOfPhysicalNodes = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_PHYSICAL_NODES));
        float initialWeight = Float.valueOf(rb.getString(PROPERTY_INITIAL_WEIGHT));
        int numberOfRushLevel = Integer.valueOf(rb.getString(PROPERTY_NUMBER_OF_RUSH_LEVEL));
        int clusterCapacity = Integer.valueOf(rb.getString(PROPERTY_CLUSTER_CAPACITY));
        String[] rushLevelNames = rb.getString(PROPERTY_RUSH_LEVEL_NAMES).split(",");

        int totalCapacity = (int)Math.pow(clusterCapacity, numberOfRushLevel);
        if (numberOfPhysicalNodes > totalCapacity) {
            SimpleLog.i("Number of physical nodes exceeds the total capacity of current settings!");
            return;
        }
        int avgPhysicalNodesPerCluster = (int)Math.ceil(numberOfPhysicalNodes / Math.pow(clusterCapacity, numberOfRushLevel - 1));

        int lastDot = startIp.lastIndexOf(".") + 1;
        String ipPrefix = startIp.substring(0, lastDot);
        int intStartIp = Integer.valueOf(startIp.substring(lastDot));

        Queue<Integer> ipPool = MathX.nonrepeatRandom(ipRange, numberOfPhysicalNodes);
        Queue<Integer> portPool = MathX.nonrepeatRandom(portRange, numberOfPhysicalNodes);

        generateMap(map.getRoot(),
                1,
                ipPool,
                portPool,
                rushLevelNames,
                numberOfRushLevel,
                initialWeight,
                clusterCapacity,
                ipPrefix,
                intStartIp,
                startPort,
                avgPhysicalNodesPerCluster);

        SimpleLog.i("Allocating placement groups...");
        allocatePlacementGroups(map);

        SimpleLog.i("Placement groups allocated...");
        SimpleLog.i("Map initialized...");
    }

    private void generateMap(Clusterable cluster, int currentLevel,
                            Queue<Integer> ipPool, Queue<Integer> portPool,
                            String[] rushLevelNames, int numberOfRushLevel,
                            float initialWeight, int clusterCapacity,
                            String ipPrefix, int intStartIp, int startPort,
                            int avgPhysicalNodesPerCluster) {
        cluster.setId(rushLevelNames[currentLevel - 1]);
        cluster.setStatus(STATUS_ACTIVE);
        cluster.setSubClusters(new Clusterable[clusterCapacity]);
        cluster.setWeight(0);

        for (int i = 0; i < cluster.getSubClusters().length; i++){
            if (currentLevel + 1 < numberOfRushLevel) {
                cluster.getSubClusters()[i] = new Cluster();
                generateMap(cluster.getSubClusters()[i],
                        currentLevel + 1,
                        ipPool,
                        portPool,
                        rushLevelNames,
                        numberOfRushLevel,
                        initialWeight,
                        clusterCapacity,
                        ipPrefix,
                        intStartIp,
                        startPort,
                        avgPhysicalNodesPerCluster);

                if (cluster.getSubClusters()[i] != null)
                    cluster.setWeight(cluster.getWeight() + cluster.getSubClusters()[i].getWeight());
            }
            else {
                Integer ip = ipPool.poll();
                Integer port = portPool.poll();

                if (i >= avgPhysicalNodesPerCluster || ip == null || port == null)
                    return;

                PhysicalNode node = new PhysicalNode();
                node.setAddress(ipPrefix + (intStartIp + ip));
                node.setPort(startPort + port);
                node.setWeight(initialWeight);
                cluster.getSubClusters()[clusterCapacity - i - 1] = node;
            }
        }
    }

    private void allocatePlacementGroups(ClusterMap map) {
        for (int i = 0; i< NUMBER_OF_PLACEMENT_GROUPS; i++) {
            int r = 0;
            int count = 0;
            String pgid = map.getPlacementGroupId(i);

            while (count < NUMBER_OF_REPLICAS) {
                Clusterable node = map.rush(pgid, r++);

                if (node != null &&
                        node.getStatus().equals(STATUS_ACTIVE) &&
                        (node instanceof PhysicalNode)) {
                    PhysicalNode pnode = (PhysicalNode)node;
                    pnode.getVirtualNodes().add(new PlacementGroup(pgid, r));
                    count++;
                }
            }
        }
    }

    public void addPhysicalNode(ClusterMap map, String clusterId, PhysicalNode node) {
        if (map.getPhysicalNodeMap().containsKey(node.getId())) {
            SimpleLog.i(node.getId() + " already exists. Try a different ip:port");
            return;
        }

        SimpleLog.i("Adding new physical node: " + node.toString() + "...");

        Clusterable cluster = map.findCluster(clusterId);
        if (cluster == null) {
            SimpleLog.i(clusterId + " not found!");
            return;
        }

        float totalWeight = 0;
        int i;
        for (i = cluster.getSubClusters().length - 1; i >= 0; i--) {
            if (cluster.getSubClusters()[i] == null) {
                cluster.getSubClusters()[i] = node;
                break;
            }
            else {
                totalWeight += cluster.getSubClusters()[i].getWeight();
            }
        }
        if (i == 0) {
            SimpleLog.i("Desired cluster is out of its capacity!");
            return;
        }

        map.getPhysicalNodeMap().put(node.getId(), node);
        float redistributeWeight = totalWeight / (cluster.getSubClusters().length - i);
        for (i = cluster.getSubClusters().length - 1; i >= 0; i--) {
            if (cluster.getSubClusters()[i] != null) {
                cluster.getSubClusters()[i].setWeight(redistributeWeight);
            }
        }
        map.loadBalancing(cluster);

        SimpleLog.i("Physical node added...");
    }

    public void removePhysicalNode(ClusterMap map, PhysicalNode node) {
        SimpleLog.i("Remove physical node: " + node.toString() + "...");

        PhysicalNode pnode = map.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }

        Clusterable cluster = map.findCluster(node.getId());
        if (cluster == null) {
            SimpleLog.i("Physical node list is outdated, " + node.getId() + " does not exist.");
            return;
        }

        cluster.setStatus(STATUS_INACTIVE);
        map.onNodeFailureOrRemoval(node);
        SimpleLog.i("Physical node removed...");
    }
}
