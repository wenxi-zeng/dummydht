package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import util.Config;
import util.SimpleLog;

import java.util.LinkedList;
import java.util.Queue;

import static util.Config.STATUS_ACTIVE;
import static util.Config.STATUS_INACTIVE;

public class CephMembershipAlgorithm {

    public void initialize(ClusterMap map) {
        SimpleLog.i("Initializing map...");

        Config config = Config.getInstance();

        String[] nodes = config.getNodes();
        int startPort = config.getStartPort();
        int portRange = config.getPortRange();
        int numberOfActiveNodes = config.getInitNumberOfActiveNodes();
        float initialWeight = config.getInitialWeight();
        int numberOfRushLevel = config.getNumberOfRushLevel();
        int clusterCapacity = config.getClusterCapacity();
        String[] rushLevelNames = config.getRushLevelNames();

        int totalCapacity = (int)Math.pow(clusterCapacity, numberOfRushLevel - 1);
        if (numberOfActiveNodes > totalCapacity) {
            SimpleLog.i("Number of physical nodes exceeds the total capacity of current settings!");
            return;
        }
        int avgPhysicalNodesPerCluster = (int)Math.ceil(numberOfActiveNodes / Math.pow(clusterCapacity, numberOfRushLevel - 2));

        Queue<PhysicalNode> pnodes = new LinkedList<>(); // use for reference when generate table
        int counter = 0;
        outerloop:
        for (int port = startPort; port < startPort + portRange; port++) {
            for (String ip : nodes) {
                PhysicalNode node = new PhysicalNode();
                node.setAddress(ip);
                node.setPort(port);
                pnodes.add(node);
                node.setWeight(initialWeight);

                if (++counter >= numberOfActiveNodes)
                    break outerloop;
            }
        }

        generateMap(
                map,
                map.getRoot(),
                pnodes,
                1,
                rushLevelNames,
                numberOfRushLevel,
                initialWeight,
                clusterCapacity,
                avgPhysicalNodesPerCluster,
                (int)Math.pow(clusterCapacity, numberOfRushLevel - 1));

        SimpleLog.i("Allocating placement groups...");
        allocatePlacementGroups(map);
        SimpleLog.i("Placement groups allocated...");

        SimpleLog.i("Map initialized...");

        if (map.getMembershipCallBack() != null)
            map.getMembershipCallBack().onInitialized();
    }

    private void generateMap(ClusterMap map, Clusterable cluster,
                            Queue<PhysicalNode> nodePool, int currentLevel,
                            String[] rushLevelNames, int numberOfRushLevel,
                            float initialWeight, int clusterCapacity,
                            int avgPhysicalNodesPerCluster, int id) {
        cluster.setId(rushLevelNames[currentLevel - 1] + id);
        cluster.setStatus(STATUS_ACTIVE);
        cluster.setSubClusters(new Clusterable[clusterCapacity]);
        cluster.setWeight(0);

        for (int i = 0; i < cluster.getSubClusters().length; i++){
            if (currentLevel + 1< numberOfRushLevel) {
                cluster.getSubClusters()[i] = new Cluster();
                generateMap(
                        map,
                        cluster.getSubClusters()[i],
                        nodePool,
                        currentLevel + 1,
                        rushLevelNames,
                        numberOfRushLevel,
                        initialWeight,
                        clusterCapacity,
                        avgPhysicalNodesPerCluster,
                        --id);

                if (cluster.getSubClusters()[i] != null)
                    cluster.setWeight(cluster.getWeight() + cluster.getSubClusters()[i].getWeight());
            }
            else {
                if (i >= avgPhysicalNodesPerCluster || nodePool.peek() == null)
                    return;

                PhysicalNode node = nodePool.poll();
                cluster.getSubClusters()[clusterCapacity - i - 1] = node;
                cluster.setWeight(cluster.getWeight() + initialWeight);
                map.getPhysicalNodeMap().put(node.getId(), node);
            }
        }
    }

    private void allocatePlacementGroups(ClusterMap map) {
        for (int i = 0; i< Config.getInstance().getNumberOfPlacementGroups(); i++) {
            int r = 0;
            int count = 0;
            PlacementGroup pg = new PlacementGroup(i, r);

            while (count < Config.getInstance().getNumberOfReplicas()) {
                Clusterable node = map.rush(pg.getId(), r);

                if (node != null &&
                        node.getStatus().equals(STATUS_ACTIVE) &&
                        (node instanceof PhysicalNode)) {
                    PhysicalNode pnode = (PhysicalNode)node;
                    pg.setIndex(r);
                    if (!pnode.getVirtualNodes().contains(pg)) {
                        pnode.getVirtualNodes().add(pg);
                        count++;
                    }
                }

                r += 1;
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

        int i;
        for (i = cluster.getSubClusters().length - 1; i >= 0; i--) {
            if (cluster.getSubClusters()[i] == null) {
                cluster.getSubClusters()[i] = node;
                break;
            }
        }
        if (i < 0) {
            SimpleLog.i("Desired cluster is out of its capacity!");
            return;
        }

        map.getPhysicalNodeMap().put(node.getId(), node);
        map.getWeightDistributeStrategy().onNodeAddition(map, cluster, node);
        //map.loadBalancing(cluster);
        map.loadBalancing(map.getRoot());

        SimpleLog.i("Physical node added...");
    }

    public void removePhysicalNode(ClusterMap map, PhysicalNode node) {
        SimpleLog.i("Remove physical node: " + node.toString() + "...");

        PhysicalNode pnode = map.getPhysicalNodeMap().get(node.getId());
        if (pnode == null) {
            SimpleLog.i(node.getId() + " does not exist.");
            return;
        }
        else if (pnode.getStatus().equals(STATUS_INACTIVE)) {
            SimpleLog.i(node.getId() + " has already been removed or marked as failure");
            return;
        }

        pnode.setStatus(STATUS_INACTIVE);
        map.onNodeFailureOrRemoval(pnode);
        SimpleLog.i("Physical node removed...");
    }
}
