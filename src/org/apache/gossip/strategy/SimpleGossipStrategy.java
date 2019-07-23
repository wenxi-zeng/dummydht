package org.apache.gossip.strategy;

import org.apache.gossip.LocalMember;
import org.apache.gossip.manager.GossipManager;

import java.util.ArrayList;
import java.util.List;

public class SimpleGossipStrategy extends GossipStrategy {

    protected SimpleGossipStrategy(GossipManager gossipManager) {
        super(gossipManager);
    }

    @Override
    public List<LocalMember> getGossipMembers() {
        return gossipManager.getLiveMembers();
    }

    @Override
    public List<LocalMember> getWatchMemberSet() {
        return new ArrayList<>(gossipManager.getMembers().keySet());
    }

}
