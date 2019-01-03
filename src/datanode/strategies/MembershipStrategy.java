package datanode.strategies;

import commonmodels.DataNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Response;
import socket.SocketClient;
import util.SimpleLog;

import java.net.URISyntaxException;
import java.net.UnknownHostException;

public abstract class MembershipStrategy {

    protected DataNode dataNode;

    public MembershipStrategy(DataNode dataNode) {
        this.dataNode = dataNode;
    }

    public abstract Response getMembersStatus();

    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException, InvalidRequestException {
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

                if ("Datanode server is not started".equals(o)) {
                    onFailure("Datanode server is not started");
                }
                else {
                    dataNode.updateTable(o);
                    fetched[0] = true;
                }
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.i(error);
            }
        };

        for (String seed : dataNode.getSeeds()) {
            if (!seed.equals(dataNode.getAddress()) && !seed.equals(dataNode.getLocalAddress())) {
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
