package datanode;

import ceph.CephDataNode;
import com.codahale.metrics.MetricRegistry;
import commonmodels.DataNode;
import elastic.ElasticDataNode;
import org.apache.gossip.*;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import ring.RingDataNode;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataNodeServer implements GossipListener {

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
                this, new MetricRegistry());
    }

    public void start() throws Exception {
        gossipService.start();
    }

    public void stop() {
        gossipService.shutdown();
        dataNode.destroy();
    }

    public String getMembersStatus() {
        return printLiveMembers() + printDeadMembers();
    }

    private String printLiveMembers() {
        List<LocalGossipMember> members = gossipService.getGossipManager().getLiveMembers();
        return getMemberStatus("Live: None\n", members);
    }

    private String printDeadMembers() {
        List<LocalGossipMember> members = gossipService.getGossipManager().getDeadMembers();
        return getMemberStatus("Dead: None\n", members);
    }

    public void processCommand(String[] args) {
        dataNode.getTerminal().execute(args);
    }

    @Override
    public void gossipEvent(GossipMember gossipMember, GossipState gossipState) {
        switch (gossipState) {
            case UP:
                dataNode.onNodeUp(gossipMember.getClusterName(), gossipMember.getUri().getHost(), gossipMember.getUri().getPort());
                break;
            case DOWN:
                dataNode.onNodeDown(gossipMember.getUri().getHost(), gossipMember.getUri().getPort());
                break;
        }
    }

    private String getMemberStatus(String valueIfNone, List<LocalGossipMember> members) {
        StringBuilder result = new StringBuilder();
        result.append(valueIfNone);

        if (members.isEmpty()) {
            return result.toString();
        }

        for (LocalGossipMember member : members)
            result.append(member.getId()).append(" ").append(member.getHeartbeat()).append('\n');

        return result.toString();
    }
}
