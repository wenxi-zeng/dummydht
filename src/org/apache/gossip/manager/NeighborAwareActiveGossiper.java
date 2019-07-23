package org.apache.gossip.manager;

import com.codahale.metrics.MetricRegistry;
import org.apache.gossip.LocalMember;

import java.util.List;
import java.util.concurrent.*;

public class NeighborAwareActiveGossiper extends AbstractActiveGossiper {

    private ScheduledExecutorService scheduledExecutorService;
    private ThreadPoolExecutor threadService;

    public NeighborAwareActiveGossiper(GossipManager gossipManager, GossipCore gossipCore,
                                MetricRegistry registry) {
        super(gossipManager, gossipCore, registry);
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(1024);
        threadService = new ThreadPoolExecutor(1, 30, 1, TimeUnit.SECONDS, workQueue,
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    @Override
    public void init() {
        super.init();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            threadService.execute(this::sendToALiveMember);
        }, 0, gossipManager.getSettings().getGossipInterval(), TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(this::sendToDeadMember, 0, gossipManager.getSettings().getGossipInterval(), TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(
                () -> sendPerNodeData(gossipManager.getMyself(),
                        selectPartner(gossipManager.getGossipMembers())),
                0, gossipManager.getSettings().getGossipInterval(), TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(
                () -> sendSharedData(gossipManager.getMyself(),
                        selectPartner(gossipManager.getGossipMembers())),
                0, gossipManager.getSettings().getGossipInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.debug("Issue during shutdown", e);
        }
        sendShutdownMessage();
        threadService.shutdown();
        try {
            threadService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.debug("Issue during shutdown", e);
        }
    }

    protected void sendToALiveMember(){
        LocalMember member = selectPartner(gossipManager.getGossipStrategy().getGossipMembers());
        sendMembershipList(gossipManager.getMyself(), member);
    }

    protected void sendToDeadMember(){
        LocalMember member = selectPartner(gossipManager.getDeadMembers());
        sendMembershipList(gossipManager.getMyself(), member);
    }

    /**
     * sends an optimistic shutdown message to several clusters nodes
     */
    protected void sendShutdownMessage(){
        List<LocalMember> l = gossipManager.getGossipStrategy().getGossipMembers();
        int sendTo = l.size() < 3 ? 1 : l.size() / 2;
        for (int i = 0; i < sendTo; i++) {
            threadService.execute(() -> sendShutdownMessage(gossipManager.getMyself(), selectPartner(l)));
        }
    }
}
