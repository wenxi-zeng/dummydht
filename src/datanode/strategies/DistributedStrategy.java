package datanode.strategies;

import commands.DaemonCommand;
import commonmodels.DataNode;
import commonmodels.NotableLoadChangeCallback;
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
import socket.SocketClient;
import util.Config;
import util.SimpleLog;
import util.URIHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DistributedStrategy extends MembershipStrategy implements GossipListener, UpdateSharedDataEventHandler, NotableLoadChangeCallback {

    private GossipManager gossipService;

    private PeerSelector selector;

    private Map<String, Long> vectorTime;

    private final long lbUpperBound;

    private final long lbLowerBound;

    public DistributedStrategy(DataNode dataNode) {
        super(dataNode);

        vectorTime = new HashMap<>();
        lbUpperBound = Config.getInstance().getLoadBalancingUpperBound();
        lbLowerBound = Config.getInstance().getLoadBalancingLowerBound();
    }

    private static final long MESSAGE_EXPIRE_TIME = 10 * 1000L;

    public static final String NODE_PROPERTY_LOAD_STATUS = "load_status";

    @Override
    public void gossipEvent(Member gossipMember, GossipState gossipState) {
        if (gossipState == GossipState.DOWN) {
            Request r = dataNode.prepareRemoveNodeCommand(
                    gossipMember.getUri().getHost(),
                    gossipMember.getUri().getPort());
            dataNode.execute(r);

            List<Request> requests = new ArrayList<>();
            requests.add(r);
            gossipRequests(requests, gossipMember.getId());
        }
    }

    @Override
    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException, InvalidRequestException {
        initGossipManager(dataNode);
        gossipService.init();
        super.onNodeStarted();
    }

    @Override
    public void onNodeStopped() {
        gossipService.shutdown();
    }

    @Override
    protected void bootstrapped() {
        selector = new PeerSelector(gossipService, socketClient);
        if (!dataNode.getPhysicalNodes().contains(new PhysicalNode(dataNode.getAddress()))) {
            Request r = dataNode.prepareAddNodeCommand();
            dataNode.execute(r);

            List<Request> requests = new ArrayList<>();
            requests.add(r);
            gossipRequests(requests, gossipService.getMyself().getId());
        }
    }

    @Override
    public Response getMembersStatus() {
        return new Response().withHeader(DaemonCommand.STATUS.name())
                .withStatus(Response.STATUS_SUCCESS)
                .withMessage(printLiveMembers() + printDeadMembers());
    }

    private void initGossipManager(DataNode dataNode) throws URISyntaxException {
        GossipSettings settings = new GossipSettings();
        settings.setWindowSize(1000);
        settings.setGossipInterval(1000);
        settings.setCleanupInterval(10 * 60 * 1000);
        settings.setConvictThreshold(5.2);
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

        // SimpleLog.i("from: " + key + ", oldValue: " + oldValue + ", newValue: " + newValue);
        @SuppressWarnings("unchecked")
        GrowOnlySet<Request> digests = (GrowOnlySet<Request>)newValue;
        List<Request> requests = getUndigestedRequests(vectorTime.getOrDefault(key, 0L), digests.value());
        for (Request r : requests) {
            // SimpleLog.i("Digesting request: " + r.toCommand());
            dataNode.execute(r);
            vectorTime.put(key, r.getTimestamp());
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
                .filter(d -> d.getTimestamp() > version)
                .sorted(Comparator.comparingLong(Request::getTimestamp))
                .collect(Collectors.toList());
    }

    @Override
    public void onLoadInfoReported(LoadInfo loadInfo) {
        int myLoadLevel = loadInfo.getLoadLevel(lbLowerBound, lbUpperBound);
        if (selector != null &&
                myLoadLevel != LoadInfo.LEVEL_VERY_HEAVY &&
                myLoadLevel != LoadInfo.LEVEL_MEDIAN_HEAVY &&
                myLoadLevel != LoadInfo.LEVEL_HEAVY ) {
            Request request = new Request().withHeader(DaemonCommand.LOADHANDSHAKE.name())
                    .withLargeAttachment(loadInfo);
            selector.start(myLoadLevel, request);
        }

        this.gossipService.getMyself().getProperties()
                .put(NODE_PROPERTY_LOAD_STATUS,
                        String.valueOf(myLoadLevel));
    }

    @Override
    public void onRequestAvailable(List<Request> request) {
        Map<String, List<Request>> map = new HashMap<>();

        SimpleLog.i("onRequestAvailable");
        for (Request r : request) {
            SimpleLog.i(r.toCommand());
            String key = r.getReceiver();
            if (key == null || key.isEmpty())
                key = gossipService.getMyself().getId();

            List<Request> requestList = map.getOrDefault(key, new ArrayList<>());
            requestList.add(r);
            map.put(key, requestList);
        }

        for (Map.Entry<String, List<Request>> entry : map.entrySet()) {
            gossipRequests(entry.getValue(), entry.getKey());
        }
    }

    private void gossipRequests(List<Request> requests, String key) {
        long now = System.currentTimeMillis();
        Set<Request> digests = new HashSet<>(requests);
        GrowOnlySet<Request> growOnlySet = new GrowOnlySet<>(digests);
        SharedDataMessage m = new SharedDataMessage();
        m.setExpireAt(now + MESSAGE_EXPIRE_TIME);
        m.setKey(key);
        m.setPayload(growOnlySet);
        m.setTimestamp(now);
        gossipService.merge(m);
    }

    class PeerSelector {

        private ExecutorService executorService;

        private ThreadPoolExecutor threadPoolExecutor;

        private final GossipManager gossipServiceRef;

        private final SocketClient socketClientRef;

        public PeerSelector(GossipManager gossipService, SocketClient socketClient) {
            this.executorService = Executors.newSingleThreadExecutor();
            BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(8);
            this.threadPoolExecutor = new ThreadPoolExecutor(1, 30, 1, TimeUnit.SECONDS, workQueue,
                    new ThreadPoolExecutor.DiscardOldestPolicy());
            this.gossipServiceRef = gossipService;
            this.socketClientRef = socketClient;
        }

        public void start(int myLoadLevel, Request request) {
            executorService.execute(() -> threadPoolExecutor.execute(() -> select(myLoadLevel, request)));
        }

        private void select(int myLoadLevel, Request request) {
            List<LoadInfo> loadInfoList = generateSortedTargets(myLoadLevel);
            if (loadInfoList == null || loadInfoList.size() < 1) return;

            AtomicBoolean selected = new AtomicBoolean(false);
            SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
                @Override
                public void onResponse(Request request, Response o) {
                    SimpleLog.i(o);

                    if (o.getStatus() == Response.STATUS_FAILED) {
                        onFailure(request, o.getMessage());
                    }
                    else {
                        selected.set(true);
                    }
                }

                @Override
                public void onFailure(Request request, String error) {
                    SimpleLog.i(error);
                }
            };

            for (LoadInfo target : loadInfoList) {
                socketClientRef.send(target.getNodeId(), request, callBack);
                if (selected.get()) break;
            }
        }

        private List<LoadInfo> generateSortedTargets(int myLoadLevel) {
            List<LoadInfo> veryHeavyNodes = new ArrayList<>();
            List<LoadInfo> medianHeavyNodes = new ArrayList<>();
            List<LoadInfo> heavyNodes = new ArrayList<>();
            List<LoadInfo> result = new ArrayList<>();

            PhysicalNode dummyNode = new PhysicalNode();
            for (Member member : gossipServiceRef.getLiveMembers()) {
                String loadStatus = member.getProperties().get(NODE_PROPERTY_LOAD_STATUS);
                if (loadStatus == null) continue;

                dummyNode.setAddress(member.getUri().getHost());
                dummyNode.setPort(member.getUri().getPort());
                LoadInfo info = new LoadInfo().withNodeId(dummyNode.getFullAddress());
                info.setReportTime(member.getHeartbeat());
                int loadLevel = Integer.valueOf(loadStatus);
                if (loadLevel == LoadInfo.LEVEL_VERY_HEAVY) {
                    veryHeavyNodes.add(info);
                }
                else if (loadLevel == LoadInfo.LEVEL_MEDIAN_HEAVY) {
                    medianHeavyNodes.add(info);
                }
                else if (loadLevel == LoadInfo.LEVEL_HEAVY) {
                    heavyNodes.add(info);
                }
            }

            Comparator<LoadInfo> heartbeatComparator = (o1, o2) -> -1 * Long.compare(o1.getReportTime(), o2.getReportTime());
            veryHeavyNodes.sort(heartbeatComparator);
            medianHeavyNodes.sort(heartbeatComparator);
            heavyNodes.sort(heartbeatComparator);

            if (myLoadLevel == LoadInfo.LEVEL_LIGHT) {
                result.addAll(heavyNodes);
                result.addAll(medianHeavyNodes);
                result.addAll(veryHeavyNodes);
                return result;
            }
            else if (myLoadLevel == LoadInfo.LEVEL_MEDIAN_LIGHT) {
                result.addAll(medianHeavyNodes);
                result.addAll(veryHeavyNodes);
                result.addAll(heavyNodes);
                return result;
            }
            else if (myLoadLevel == LoadInfo.LEVEL_VERY_LIGHT) {
                result.addAll(veryHeavyNodes);
                result.addAll(medianHeavyNodes);
                result.addAll(heavyNodes);
                return result;
            }
            else {
                return null;
            }
        }
    }
}
