package datanode;

import ceph.CephDataNode;
import com.codahale.metrics.MetricRegistry;
import commonmodels.DataNode;
import elastic.ElasticDataNode;
import org.apache.gossip.*;
import ring.RingDataNode;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataNodeServer {

    private GossipService gossipService;

    private DataNode dataNode;

    public DataNodeServer(String type, int port) throws Exception {
        initDataNode(type, port);
        initGossipManager(dataNode);
    }

    private void initDataNode(String type, int port) throws Exception {
        if (type.equalsIgnoreCase("ring"))
            dataNode = new RingDataNode();
        else if (type.equalsIgnoreCase("elastic"))
            dataNode = new ElasticDataNode();
        else if (type.equalsIgnoreCase("ceph"))
            dataNode = new CephDataNode();
        else
            throw new Exception("Invalid DHT type");

        dataNode.setPort(port);
        dataNode.setUseDynamicAddress(true);
    }

    private void initGossipManager(DataNode dataNode) throws URISyntaxException, UnknownHostException, InterruptedException {
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

    public void start() throws Exception {
        gossipService.start();
    }

    public void stop() {
        gossipService.shutdown();
        dataNode.destroy();
    }

    public String getMembersStatus() {
        return printLiveMembers() + "\n" + printDeadMambers();
    }

    private String printLiveMembers() {
        List<LocalGossipMember> members = gossipService.getGossipManager().getLiveMembers();
        StringBuilder result = new StringBuilder();

        if (members.isEmpty()) {
            result.append("Live: (none)");
            return result.toString();
        }
        result.append("Live: ").append(members.get(0));
        for (int i = 1; i < members.size(); i++) {
            result.append("    : ").append(members.get(i));
        }

        return result.toString();
    }

    private String printDeadMambers() {
        List<LocalGossipMember> members = gossipService.getGossipManager().getDeadMembers();
        StringBuilder result = new StringBuilder();

        if (members.isEmpty()) {
            result.append("Dead: (none)");
            return result.toString();
        }
        result.append("Dead: ").append(members.get(0));
        for (int i = 1; i < members.size(); i++) {
            result.append("    : ").append(members.get(i));
        }

        return result.toString();
    }

    public void processCommand(String[] args) {
        dataNode.getTerminal().execute(args);
    }
}
