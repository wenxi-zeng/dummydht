package datanode.strategies;

import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import loadmanagement.LoadInfo;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.RemoteMember;
import org.apache.gossip.crdt.GrowOnlySet;
import org.apache.gossip.event.GossipListener;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.event.data.UpdateSharedDataEventHandler;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.apache.gossip.model.SharedDataMessage;
import util.Config;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class DistributedStrategy extends MembershipStrategy implements GossipListener, UpdateSharedDataEventHandler {

    private GossipManager gossipService;

    private Map<String, Long> vectorTime;

    private final long lbUpperBound;

    private final long lbLowerBound;

    public DistributedStrategy(DataNode dataNode) {
        super(dataNode);

        vectorTime = new HashMap<>();
        lbUpperBound = Config.getInstance().getLoadBalancingUpperBound();
        lbLowerBound = Config.getInstance().getLoadBalancingLowerBound();
    }

    private static final long MESSAGE_EXPIRE_TIME = 10 * 60 * 1000L;

    public static final String NODE_PROPERTY_LOAD_STATUS = "load_status";

    @Override
    public void gossipEvent(Member gossipMember, GossipState gossipState) {

    }

    @Override
    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException, InvalidRequestException {
        super.onNodeStarted();
        initGossipManager(dataNode);
        gossipService.init();

        if (!dataNode.getPhysicalNodes().contains(new PhysicalNode(dataNode.getAddress()))) {
            dataNode.execute(
                    dataNode.prepareAddNodeCommand().toCommand()
            );

            long now = System.currentTimeMillis();
            Set<Request> digests = new HashSet<>();
            digests.add(dataNode.prepareAddNodeCommand());
            GrowOnlySet<Request> growOnlySet = new GrowOnlySet<>(digests);
            SharedDataMessage m = new SharedDataMessage();
            m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
            m.setKey(gossipService.getMyself().getId());
            m.setPayload(growOnlySet);
            m.setTimestamp(now);
            gossipService.merge(m);
        }
    }

    @Override
    public void onNodeStopped() {
        gossipService.shutdown();
    }

    @Override
    public Response getMembersStatus() {
        return new Response().withStatus(Response.STATUS_SUCCESS)
                    .withMessage(printLiveMembers() + printDeadMembers());
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

    @Override
    public void onUpdate(String key, Object oldValue, Object newValue) {
        if (newValue == null) return;

        @SuppressWarnings("unchecked")
        GrowOnlySet<Request> digests = (GrowOnlySet<Request>)newValue;
        List<Request> requests = getUndigestedRequests(vectorTime.get(key), digests.value());
        for (Request r : requests) {
            dataNode.execute(r);
            vectorTime.put(key, r.getEpoch());
        }
    }

    private Request getMaxDigest(Set<Request> digests) {
        long version = -1;
        Request digest = null;

        for (Request r : digests) {
            if (r.getEpoch() > version) {
                version = r.getEpoch();
                digest = r;
            }
        }

        return digest;
    }

    private List<Request> getUndigestedRequests(long version, Set<Request> digests) {
        return digests.stream()
                .filter(d -> d.getEpoch() > version)
                .sorted(Comparator.comparingLong(Request::getEpoch))
                .collect(Collectors.toList());
    }

    @Override
    public void onLoadInfoReported(LoadInfo loadInfo) {
        this.gossipService.getMyself().getProperties()
                .put(NODE_PROPERTY_LOAD_STATUS,
                        String.valueOf(loadInfo.getLoadLevel(lbLowerBound, lbUpperBound)));
    }
}
