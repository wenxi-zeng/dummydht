package datanode.strategies;

import commonmodels.CommonCommand;
import commonmodels.DataNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import socket.SocketClient;
import util.SimpleLog;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

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
        AtomicBoolean fetched = new AtomicBoolean(false);
        SocketClient socketClient = new SocketClient();
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Response o) {
                SimpleLog.i(String.valueOf(o));

                if (o.getStatus() == Response.STATUS_FAILED) {
                    onFailure(o.getMessage());
                }
                else {
                    dataNode.updateTable(o.getAttachment());
                    fetched.set(true);
                }
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.i(error);
            }
        };

        for (String seed : dataNode.getSeeds()) {
            if (!seed.equals(dataNode.getAddress()) && !seed.equals(dataNode.getLocalAddress())) {
                Request request = new Request().withHeader(CommonCommand.FETCH.name());
                socketClient.send(seed, request, callBack);
            }
            if (fetched.get()) break;
        }

        if (!fetched.get()) {
            SimpleLog.i("Creating table");
            dataNode.createTable();
        }
    }
}
