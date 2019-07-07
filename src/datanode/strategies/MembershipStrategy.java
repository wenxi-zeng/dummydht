package datanode.strategies;

import commands.DaemonCommand;
import commonmodels.DataNode;
import commonmodels.LoadInfoReportHandler;
import commonmodels.MembershipCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import socket.SocketClient;
import util.SimpleLog;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class MembershipStrategy implements LoadInfoReportHandler, MembershipCallBack {

    protected DataNode dataNode;

    protected SocketClient socketClient;

    public MembershipStrategy(DataNode dataNode) {
        this.dataNode = dataNode;
        this.socketClient = SocketClient.getInstance();
    }

    public abstract Response getMembersStatus();

    public void onNodeStarted() throws InterruptedException, UnknownHostException, URISyntaxException, InvalidRequestException {
        // in case bootstrap takes a long time to response, wrap it with a thread
        new Thread(this::bootstrap).start();
    }

    public void onNodeStopped() {
        // stub
    }

    private void bootstrap() {
        AtomicBoolean fetched = new AtomicBoolean(false);
        Semaphore semaphore = new Semaphore(0);
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Request request, Response o) {
                SimpleLog.i(o);

                if (o.getStatus() == Response.STATUS_FAILED) {
                    onFailure(request, o.getMessage());
                }
                else {
                    dataNode.createTable(o.getAttachment());
                    fetched.set(true);
                    semaphore.release();
                }
            }

            @Override
            public void onFailure(Request request, String error) {
                SimpleLog.i(error);
                semaphore.release();
            }
        };

        for (String seed : dataNode.getSeeds()) {
            if (!seed.equals(dataNode.getAddress()) && !seed.equals(dataNode.getLocalAddress())) {
                Request request = new Request().withHeader(DaemonCommand.FETCH.name())
                                                .withFollowup(dataNode.getAddress());
                socketClient.send(seed, request, callBack);
                try {
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (fetched.get()) break;
        }

        if (!fetched.get()) {
            SimpleLog.i("Creating table");
            dataNode.setMembershipCallBack(this);
            dataNode.createTable();
        }
        bootstrapped();
    }

    protected void bootstrapped() {
        // stub
    }

    @Override
    public void onInitialized() {
        Request request = new Request()
                .withHeader(DaemonCommand.START.name());
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Request request, Response o) {
                SimpleLog.i(o);
            }

            @Override
            public void onFailure(Request request, String error) {
                SimpleLog.i(error);
            }
        };
        for (PhysicalNode node : dataNode.getPhysicalNodes()) {
            if (!node.getFullAddress().equals(dataNode.getAddress()) && !node.getFullAddress().equals(dataNode.getLocalAddress())) {
                socketClient.send(node.getAddress(), node.getPort(), request, callBack);
            }
        }
    }
}
