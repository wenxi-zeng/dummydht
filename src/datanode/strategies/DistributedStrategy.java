package datanode.strategies;

import commonmodels.DataNode;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.RemoteMember;
import org.apache.gossip.crdt.SharedMessage;
import org.apache.gossip.crdt.SharedMessageOrSet;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.apache.gossip.model.SharedDataMessage;
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

    private static final String INDEX_KEY_FOR_SET = "membership_update";

    private static final long MESSAGE_EXPIRE_TIME = 10 * 60 * 1000L;

    @Override
    public void gossipEvent(Member gossipMember, GossipState gossipState) {
        SharedMessage message = new SharedMessage()
                .withSender(dataNode.getAddress())
                .withSubject(gossipMember.getUri().getHost() + ":" + gossipMember.getUri().getPort());
        SharedMessage pre;

        switch (gossipState) {
            case UP:
                pre = bringUpFromTombstone(message);
                if (pre != null) {
                    message.setContent(pre.getContent());
                    addMessage(message);
                }
                break;
            case DOWN:
                pre = bringDownFromElements(message);
                if (pre != null) {
                    message.setContent(pre.getContent());
                    removeMessage(message);
                }
                break;
        }
    }

    @Override
    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException {
        super.onNodeStarted();
        initGossipManager(dataNode);
        gossipService.init();
        SharedMessage message = new SharedMessage()
                .withSender(dataNode.getAddress())
                .withSubject(dataNode.getAddress())
                .withContent(dataNode.prepareAddNodeCommand());
        addMessage(message);
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
        settings.setPersistDataState(false);
        settings.setPersistRingState(false);
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
        this.gossipService.registerSharedDataSubscriber((key, oldValue, newValue) -> {
            if (newValue == null) return;

            @SuppressWarnings("unchecked")
            SharedMessageOrSet newSet = (SharedMessageOrSet)newValue;
            System.out.println("Elements:");
            for (SharedMessage message : newSet.getElements().keySet()) {
                System.out.println(message.toStringX());
            }
            System.out.println("Tombstones:");
            for (SharedMessage message : newSet.getTombstones().keySet()) {
                System.out.println(message.toStringX());
            }
            System.out.println("Result:");
            for (SharedMessage message : newSet.value()) {
                System.out.println(message.toStringX());
            }
            System.out.println("==========================================================================");
        });
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

    private SharedMessage bringUpFromTombstone(SharedMessage target) {
        @SuppressWarnings("unchecked")
        SharedMessageOrSet s = (SharedMessageOrSet) gossipService.findCrdt(INDEX_KEY_FOR_SET);
        if (s == null || s.value().contains(target)) return null;

        for (SharedMessage message : s.getTombstones().keySet())
            if (message.equals(target))
                return message;

       return null;
    }

    private SharedMessage bringDownFromElements(SharedMessage target) {
        @SuppressWarnings("unchecked")
        SharedMessageOrSet s = (SharedMessageOrSet) gossipService.findCrdt(INDEX_KEY_FOR_SET);
        if (s == null || !s.value().contains(target)) return null;

        for (SharedMessage message : s.value())
            if (message.equals(target))
                return message;

        return null;
    }

    private void removeMessage(SharedMessage message) {
        @SuppressWarnings("unchecked")
        SharedMessageOrSet s = (SharedMessageOrSet) gossipService.findCrdt(INDEX_KEY_FOR_SET);
        if (s == null) return;

        long now = System.currentTimeMillis();
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(INDEX_KEY_FOR_SET);
        m.setPayload(new SharedMessageOrSet(s, s.remove(message)));
        m.setTimestamp(System.currentTimeMillis());
        gossipService.merge(m);
    }

    private void addMessage(SharedMessage message) {
        long now = System.currentTimeMillis();
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(INDEX_KEY_FOR_SET);
        m.setPayload(new SharedMessageOrSet(message));
        m.setTimestamp(now);
        gossipService.merge(m);
    }
}
