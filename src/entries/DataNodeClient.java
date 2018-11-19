package entries;

import ceph.CephDataNode;
import com.codahale.metrics.MetricRegistry;
import commonmodels.DataNode;
import elastic.ElasticDataNode;
import org.apache.gossip.GossipMember;
import org.apache.gossip.GossipService;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.RemoteGossipMember;
import ring.RingDataNode;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataNodeClient {

    private GossipService gossipService = null;

    private DataNode dataNode;

    public static void main(String[] args){
        if (args.length < 2) {
            SingleNodeClient.main(args);
        }
        else {
            try {
                DataNodeClient client = new DataNodeClient(args);
                client.exec();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private DataNodeClient(String[] args) throws Exception {
        initGossipManager(args);
        initDataNode(args);
    }

    private void initDataNode(String[] args) throws Exception {
        String type = args[1];

        if (type.equalsIgnoreCase("ring"))
            dataNode = new RingDataNode();
        else if (type.equalsIgnoreCase("elastic"))
            dataNode = new ElasticDataNode();
        else if (type.equalsIgnoreCase("ceph"))
            dataNode = new CephDataNode();
        else
            throw new Exception("Invalid DHT type");

        if (args.length > 2) {
            dataNode.setPort(Integer.valueOf(args[2]));
            dataNode.setUseDynamicAddress(true);
        }
    }

    private void initGossipManager(String[] args) throws URISyntaxException, UnknownHostException, InterruptedException {
        GossipSettings settings = new GossipSettings();
        List<GossipMember> startupMembers = new ArrayList<>();

        for (String seed : dataNode.getSeeds()) {
            URI uri = URIHelper.getGossipURI(seed);
            startupMembers.add(new RemoteGossipMember(
                    dataNode.getClusterName(),
                    uri,
                    dataNode.getAddress()));
        }

        this.gossipService = new GossipService(
                dataNode.getClusterName(),
                URIHelper.getGossipURI(dataNode.getAddress()),
                dataNode.getAddress(), new HashMap<>(),
                startupMembers, settings,
                (a, b) -> {}, new MetricRegistry());
    }

    private void exec() {
        gossipService.start();
    }
}
