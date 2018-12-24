package datanode.strategies;

import commonmodels.DataNode;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.RemoteMember;
import org.apache.gossip.crdt.GrowOnlyCounter;
import org.apache.gossip.crdt.OrSet;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.apache.gossip.model.SharedDataMessage;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;

public class DistributedStrategy extends MembershipStrategy implements GossipListener {

    private GossipManager gossipService;

    public DistributedStrategy(DataNode dataNode) {
        super(dataNode);
    }

    private static final String INDEX_KEY_FOR_SET = "membership_update";

    private static final long MESSAGE_EXPIRE_TIME = 10 * 60 * 1000L;

    @Override
    public void gossipEvent(Member gossipMember, GossipState gossipState) {
        switch (gossipState) {
            case DOWN:
                countMessage(dataNode.prepareRemoveNodeCommand(gossipMember.getUri().getHost(), gossipMember.getUri().getPort()));
                break;
        }
    }

    @Override
    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException {
        super.onNodeStarted();
        initGossipManager(dataNode);
        gossipService.init();
        String message = dataNode.prepareAddNodeCommand();
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

            System.out.println(
                    "Event Handler fired for key = '" + key + "'! " + oldValue + " " + newValue);
            if (newValue instanceof GrowOnlyCounter){
                GrowOnlyCounter counter = (GrowOnlyCounter) newValue;

                if (counter.value() >= gossipService.getLiveMembers().size()) {
                    if (counter.value() == gossipService.getLiveMembers().size() && key.contains("removeNode")) {
                        addMessage(key);
                    }
                    else {
                        removeMessage(key);
                        System.out.println("          message to remove: " + key);
                    }
                }
            }
            else {
                @SuppressWarnings("unchecked")
                OrSet<String> orSet = (OrSet<String>) newValue;

                for (String message : orSet.value()) {
                    dataNode.execute(message);
                    countMessage(message);
                }
            }
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

    private void removeMessage(String val) {
        @SuppressWarnings("unchecked")
        OrSet<String> s = (OrSet<String>) gossipService.findCrdt(INDEX_KEY_FOR_SET);
        if (s == null) return;

        long now = System.currentTimeMillis();
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(INDEX_KEY_FOR_SET);
        m.setPayload(new OrSet<>(s, new OrSet.Builder<String>().remove(val)));
        m.setTimestamp(System.currentTimeMillis());
        gossipService.merge(m);
    }

    private void removeMessage(List<String> val) {
        OrSet.Builder<String> builder = new OrSet.Builder<>();
        for (String str : val) {
            builder.remove(str);
        }

        @SuppressWarnings("unchecked")
        OrSet<String> s = (OrSet<String>) gossipService.findCrdt(INDEX_KEY_FOR_SET);
        long now = System.currentTimeMillis();
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(INDEX_KEY_FOR_SET);
        m.setPayload(new OrSet<>(s, builder));
        m.setTimestamp(System.currentTimeMillis());
        gossipService.merge(m);
    }

    private void addMessage(String val) {
        long now = System.currentTimeMillis();
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(INDEX_KEY_FOR_SET);
        m.setPayload(new OrSet<>(val));
        m.setTimestamp(now);
        gossipService.merge(m);
    }

    private void countMessage(String key) {
        GrowOnlyCounter c = (GrowOnlyCounter) gossipService.findCrdt(key);
        if (c == null) {
            c = new GrowOnlyCounter(new GrowOnlyCounter.Builder(gossipService).increment((1L)));
        } else if (c.getCounters().getOrDefault(key, 0L) < 1){
            c = new GrowOnlyCounter(c, new GrowOnlyCounter.Builder(gossipService).increment((1L)));
        } else {
            return;
        }

        long now = System.currentTimeMillis();
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(key);
        m.setPayload(c);
        m.setTimestamp(System.currentTimeMillis());
        gossipService.merge(m);
    }

    private void resetCounter(String key) {
        GrowOnlyCounter c = (GrowOnlyCounter) gossipService.findCrdt(key);

        if (c != null) {
            c.getCounters().clear();
        }
    }
}
