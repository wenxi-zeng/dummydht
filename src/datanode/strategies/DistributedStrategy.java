package datanode.strategies;

import commonmodels.DataNode;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.RemoteMember;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class DistributedStrategy extends MembershipStrategy implements GossipListener {

    private GossipManager gossipService;

    public DistributedStrategy(DataNode dataNode) {
        super(dataNode);
    }

    @Override
    public void gossipEvent(Member gossipMember, GossipState gossipState) {
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
        super.onNodeStarted();
        initGossipManager(dataNode);
        gossipService.init();
    }

    @Override
    public void onNodeStopped() {
        gossipService.shutdown();
    }

    @Override
    public String getMembersStatus() {
        return printLiveMembers() + printDeadMembers();
    }

    private void initGossipManager(DataNode dataNode) throws URISyntaxException {
        GossipSettings settings = new GossipSettings();
        settings.setWindowSize(1000);
        settings.setGossipInterval(1000);
        List<Member> startupMembers = new ArrayList<>();

        for (String seed : dataNode.getSeeds()) {
            URI uri = URIHelper.getGossipURI(seed);
            startupMembers.add(new RemoteMember(
                    dataNode.getClusterName(),
                    uri,
                    uri.getHost() + "." + uri.getPort()));
        }

        this.gossipService = GossipManagerBuilder.newBuilder()
                                .cluster(dataNode.getClusterName())
                                .uri(URIHelper.getGossipURI(dataNode.getAddress()))
                                .id(dataNode.getGossipId())
                                .gossipMembers(startupMembers)
                                .gossipSettings(settings)
                                .listener(this)
                                .build();
    }

    private String printLiveMembers() {
        List<LocalMember> members = gossipService.getLiveMembers();
        return getMemberStatus("Live: ", members);
    }

    private String printDeadMembers() {
        List<LocalMember> members = gossipService.getDeadMembers();
        return getMemberStatus("Dead: ", members);
    }

    private String getMemberStatus(String title, List<LocalMember> members) {
        StringBuilder result = new StringBuilder();
        result.append(title).append('\n');

        if (members.isEmpty()) {
            result.append("None\n");
        }
        else {
            for (LocalMember member : members)
                result.append("Node: ").append(member.getId()).append(" ")
                        .append("Heartbeat: ").append(member.getHeartbeat()).append('\n');
        }

        return result.toString();
    }
}
