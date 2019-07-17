package org.apache.gossip.strategy;

import org.apache.gossip.LocalMember;
import org.apache.gossip.event.GossipState;
import org.apache.gossip.manager.GossipManager;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleGossipStrategy extends GossipStrategy {

    protected SimpleGossipStrategy(GossipManager gossipManager) {
        super(gossipManager);
    }

    @Override
    public List<LocalMember> getGossipMembers() {
        return gossipManager.getLiveMembers();
    }

    @Override
    public Set<Map.Entry<LocalMember, GossipState>> getWatchMemberSet() {
        return gossipManager.getMembers().entrySet();
    }

}
