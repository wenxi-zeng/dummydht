package datanode.strategies;

import commonmodels.DataNode;
import socket.SocketClient;
import util.SimpleLog;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Iterator;

public abstract class MembershipStrategy {

    protected DataNode dataNode;

    public MembershipStrategy(DataNode dataNode) {
        this.dataNode = dataNode;
    }

    public abstract String getMembersStatus();

    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException {
        bootstrap();
    }

    public void onNodeStopped() {
        // stub
    }

    private void bootstrap() {
        Iterator<String> iterator = dataNode.getSeeds().iterator();
        SocketClient socketClient = new SocketClient();
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Object o) {
                SimpleLog.i(String.valueOf(o));
                dataNode.updateTable(o);
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.i(error);
                trySeed(iterator, socketClient, this);
            }
        };

        trySeed(iterator, socketClient, callBack);
    }

    private void trySeed(Iterator<String> iterator, SocketClient socketClient, SocketClient.ServerCallBack callBack) {
        if (iterator.hasNext()) {
            String seed = iterator.next();
            if (!seed.equals(dataNode.getAddress())) {
                socketClient.send(seed, "fetch", callBack);
            }
            else {
                trySeed(iterator, socketClient, callBack);
            }
        }
        else {
            SimpleLog.i("Creating table");
            dataNode.createTable();
        }
    }
}
