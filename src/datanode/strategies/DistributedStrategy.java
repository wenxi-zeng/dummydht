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
import org.apache.gossip.event.data.UpdateSharedDataEventHandler;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.apache.gossip.model.SharedDataMessage;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DistributedStrategy extends MembershipStrategy implements GossipListener, UpdateSharedDataEventHandler {

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
                    dataNode.execute(message.getContent());
                }
                break;
            case DOWN:
                pre = bringDownFromElements(message);
                if (pre != null) {
                    message.setContent(pre.getContent());
                    removeMessage(message);
                    dataNode.execute(
                            dataNode.prepareRemoveNodeCommand(
                                    gossipMember.getUri().getHost(),
                                    gossipMember.getUri().getPort()));
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
        dataNode.execute(message.getContent());
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
        this.gossipService.registerSharedDataSubscriber(this);
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

    private void processAddedValue(Set<SharedMessage> value) {
        for (SharedMessage message : value) {
            dataNode.execute(message.getContent());
        }
    }

    private void processRemovedValue(Set<SharedMessage> value) {
        for (SharedMessage message : value) {
            String[] address = message.getSubject().split(":");
            dataNode.execute(
                    dataNode.prepareRemoveNodeCommand(address[0], Integer.valueOf(address[1]))
            );
        }
    }

    @Override
    public void onUpdate(String key, Object oldValue, Object newValue) {

        if (oldValue == null && newValue == null) {
            return;
        }
        else if (oldValue == null) {
            @SuppressWarnings("unchecked")
            SharedMessageOrSet newSet = (SharedMessageOrSet)newValue;
            processAddedValue(newSet.value());
        }
        else if (newValue == null) {
            @SuppressWarnings("unchecked")
            SharedMessageOrSet newSet = (SharedMessageOrSet)oldValue;
            processAddedValue(newSet.value());
        }
        else if (oldValue.equals(newValue)) {
            return;
        }
        else {
            @SuppressWarnings("unchecked")
            SharedMessageOrSet newSet = (SharedMessageOrSet)newValue;

            @SuppressWarnings("unchecked")
            SharedMessageOrSet oldSet = (SharedMessageOrSet)oldValue;

            Set<SharedMessage> added = new HashSet<>(newSet.value());
            Set<SharedMessage> removed = new HashSet<>(oldSet.value());
            for (SharedMessage message : oldSet.value()) {
                added.remove(message);
            }
            for (SharedMessage message : newSet.value()) {
                removed.remove(message);
            }

            processAddedValue(added);
            processRemovedValue(removed);
        }

        System.out.println(dataNode.getTable().toString());
        System.out.println("==========================================================================");
    }
}
