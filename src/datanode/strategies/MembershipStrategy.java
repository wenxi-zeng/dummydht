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
        final boolean[] fetched = {false};
        SocketClient socketClient = new SocketClient();
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Object o) {
                SimpleLog.i(String.valueOf(o));
                dataNode.updateTable(o);
                fetched[0] = true;
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.i(error);
            }
        };

        for (String seed : dataNode.getSeeds()) {
            if (!seed.equals(dataNode.getAddress())) {
                socketClient.send(seed, "fetch", callBack);
            }
            if (fetched[0]) break;
        }

        if (!fetched[0]) {
            SimpleLog.i("Creating table");
            dataNode.createTable();
        }
    }
}
