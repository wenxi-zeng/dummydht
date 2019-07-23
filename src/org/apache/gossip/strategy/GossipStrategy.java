package org.apache.gossip.strategy;

import org.apache.gossip.LocalMember;
import org.apache.gossip.manager.GossipManager;

import java.util.List;

public abstract class GossipStrategy {

    protected final GossipManager gossipManager;

    protected GossipStrategy(GossipManager gossipManager) {
        this.gossipManager = gossipManager;
    }

    public abstract List<LocalMember> getGossipMembers();

    public abstract List<LocalMember> getWatchMemberSet();

}
