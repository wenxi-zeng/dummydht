package org.apache.gossip.strategy;

import org.apache.gossip.LocalMember;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.manager.GossipManager;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class GossipStrategy {

    protected final GossipManager gossipManager;

    protected GossipStrategy(GossipManager gossipManager) {
        this.gossipManager = gossipManager;
    }

    public abstract List<LocalMember> getGossipMembers();

    public abstract Set<Map.Entry<LocalMember, GossipState>> getWatchMemberSet();

}
