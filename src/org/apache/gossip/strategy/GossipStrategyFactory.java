package org.apache.gossip.strategy;

import org.apache.gossip.manager.GossipManager;

public class GossipStrategyFactory {

    public static GossipStrategy createStrategy(GossipManager gossipManager, String activeGossipClass) {
        if (activeGossipClass.equals("org.apache.gossip.manager.NeighborAwareActiveGossiper")) {
            return new NeighborGossipStrategy(gossipManager);
        }
        else {
            return new SimpleGossipStrategy(gossipManager);
        }
    }

}
