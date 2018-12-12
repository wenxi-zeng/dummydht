package datanode.strategies;

import com.codahale.metrics.MetricRegistry;
import commonmodels.DataNode;
import org.apache.gossip.*;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import socket.SocketClient;
import util.SimpleLog;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DistributedStrategy extends MembershipStrategy implements GossipListener {

    private GossipService gossipService;

    public DistributedStrategy(DataNode dataNode) {
        super(dataNode);
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

    @Override
    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException {
        bootstrap();
        initGossipManager(dataNode);
        gossipService.start();
    }

    @Override
    public void onNodeStopped() {
        gossipService.shutdown();
    }

    @Override
    public String getMembersStatus() {
        return printLiveMembers() + printDeadMembers();
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

    private String printLiveMembers() {
        List<LocalGossipMember> members = gossipService.getGossipManager().getLiveMembers();
        return getMemberStatus("Live: None\n", members);
    }

    private String printDeadMembers() {
        List<LocalGossipMember> members = gossipService.getGossipManager().getDeadMembers();
        return getMemberStatus("Dead: None\n", members);
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

    private void bootstrap() {
        Iterator<String> iterator = dataNode.getSeeds().iterator();
        SocketClient socketClient = new SocketClient();
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Object o) {
                SimpleLog.i(String.valueOf(o));
                dataNode.updateTable(o);
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.i(error);
                trySeed(iterator, socketClient, this);
            }
        };

        trySeed(iterator, socketClient, callBack);
    }

    private void trySeed(Iterator<String> iterator, SocketClient socketClient, SocketClient.ServerCallBack callBack) {
        if (iterator.hasNext()) {
            String seed = iterator.next();
            if (!seed.equals(dataNode.getAddress())) {
                socketClient.send(seed, "fetch", callBack);
            }
        }
        else {
            dataNode.createTable();
        }
    }
}
