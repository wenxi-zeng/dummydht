package entries;

import commands.DaemonCommand;
import commands.ProxyCommand;
import commonmodels.Daemon;
import commonmodels.LoadBalancingCallBack;
import commonmodels.MembershipCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import datanode.DataNodeServer;
import filemanagement.FileBucket;
import filemanagement.FileTransferManager;
import loadmanagement.GlobalLoadInfoManager;
import loadmanagement.LoadMonitor;
import socket.SocketClient;
import socket.SocketServer;
import statmanagement.StatInfoManager;
import util.Config;
import util.SimpleLog;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;

public class Proxy implements Daemon, LoadBalancingCallBack, MembershipCallBack, LoadMonitor.NotableLoadChangeCallback {

    private DataNodeDaemon daemon;

    private LoadMonitor loadMonitor;

    private static volatile Proxy instance = null;

    public static void main(String[] args){
        try {
            Proxy proxy = Proxy.newInstance();
            SimpleLog.with(proxy.daemon.getIp(), proxy.daemon.getPort());
            SimpleLog.i("Proxy: started");
            proxy.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Proxy() {
        Config config = Config.getInstance();
        daemon = DataNodeDaemon.newInstance(config.getSeeds().get(0));
        daemon.setSocketEventHandler(this);
    }

    public static Proxy getInstance() {
        return instance;
    }

    public static Proxy newInstance() {
        instance = new Proxy();
        return getInstance();
    }

    private void propagateTable(Request request) {
        for (PhysicalNode node : daemon.getDataNodeServer().getPhysicalNodes()) {
            send(node.getAddress(), node.getPort(), request, this);
        }
        GlobalLoadInfoManager.getInstance().update(daemon.getDataNodeServer().getPhysicalNodes());
    }

    @Override
    public void onFinished() {
        // propagate table when load balancing is done
        Request request = new Request()
                .withHeader(DaemonCommand.UPDATE.name())
                .withLargeAttachment(daemon.getDataNodeServer().getDataNodeTable());
        propagateTable(request);
        SimpleLog.v(String.valueOf(request.getLargeAttachment()));
    }

    @Override
    public void exec() throws Exception {
        daemon.exec();
    }

    @Override
    public void initDataNodeServer() throws Exception {
        daemon.initDataNodeServer();
    }

    @Override
    public void initSubscriptions() {
        daemon.getDataNodeServer().setLoadBalancingCallBack(this);
        daemon.getDataNodeServer().setMembershipCallBack(this);
        FileTransferManager.getInstance().subscribe(this);
        loadMonitor = new LoadMonitor(daemon.getDataNodeServer().getDataNode().getLoadChangeHandler());
        GlobalLoadInfoManager.getInstance().update(daemon.getDataNodeServer().getPhysicalNodes());
        GlobalLoadInfoManager.getInstance().subscribe(loadMonitor);
        loadMonitor.subscribe(this);
    }

    @Override
    public void startDataNodeServer() throws Exception {
        if (daemon.getDataNodeServer() == null) {
            initDataNodeServer();
            initSubscriptions();
            daemon.getDataNodeServer().start();
        }
        else {
            throw new Exception("Proxy is already started");
        }
    }

    @Override
    public void stopDataNodeServer() throws Exception {
        daemon.stopDataNodeServer();
    }

    @Override
    public DataNodeServer getDataNodeServer() {
        return daemon.getDataNodeServer();
    }

    @Override
    public Response processCommonCommand(Request o) {
        try {
            ProxyCommand command = ProxyCommand.valueOf(o.getHeader());
            return command.execute(o);
        }
        catch (IllegalArgumentException e) {
            return new Response(o).withStatus(Response.STATUS_INVALID_REQUEST)
                    .withMessage(e.getMessage());
        }
    }

    @Override
    public Response processDataNodeCommand(Request o) {
        return daemon.processDataNodeCommand(o);
    }

    @Override
    public void send(String address, int port, Request request, SocketClient.ServerCallBack callBack) {
        daemon.send(address, port, request, callBack);
    }

    @Override
    public void send(String address, Request request, SocketClient.ServerCallBack callBack) {
        daemon.send(address, request, callBack);
    }

    @Override
    public void onTransferring(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        daemon.onTransferring(buckets, from, toNode);
    }

    @Override
    public void onReplicating(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        daemon.onReplicating(buckets, from, toNode);
    }

    @Override
    public void onTransmitted(List<FileBucket> buckets, PhysicalNode from, PhysicalNode toNode) {
        daemon.onTransmitted(buckets, from, toNode);
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, Request o, SocketServer.EventResponsor responsor) throws Exception {
        Response response = processCommonCommand(o);
        if (response.getStatus() == Response.STATUS_INVALID_REQUEST)
            response = processDataNodeCommand(o);

        responsor.reply(out, response);
        startFollowupTask(o.getFollowup(), response);
    }

    @Override
    public void onBound() {
        // use another thread, since start a data node might invoke communication to proxy
        daemon.getExecutor().execute(() -> {
            try {
                startDataNodeServer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void onResponse(Request request, Response o) {
        SimpleLog.i(o);
    }

    @Override
    public void onFailure(Request request, String error) {
        SimpleLog.i(error);
    }

    private void startFollowupTask(String followupAddress, Response response) {
        daemon.getExecutor().execute(() -> followup(followupAddress, response));
    }

    private void followup(String followupAddress, Response response) {
        if (response.getStatus() == Response.STATUS_FAILED) return;

        if (response.getHeader().equals(ProxyCommand.ADDNODE.name())) {
            onFollowupAddNode(followupAddress, response);
        }
        else if (response.getHeader().equals(ProxyCommand.REMOVENODE.name())) {
            onFollowupRemoveNode(followupAddress, response);
        }
        else if (response.getHeader().equals(ProxyCommand.FETCH.name())) {
            onFollowupFetch(followupAddress, response);
        }
    }

    // follow up sequence:
    //      Proxy               Data Node               Control Client
    // 1                                                send ADD NODE
    // 2    receive
    // 3    send START
    // 4                        receive
    // 5                        send FETCH
    // 6    receive
    // 7    send ADD NODE
    //          to self
    // 8    load balancing
    //          & propagate table
    // 9    send UPDATE to nodes
    // 10                       receive
    private void onFollowupAddNode(String followupAddress, Response response) {
        Request request = (Request) response.getAttachment();
        send(request.getReceiver(), request, this);
    }

    // follow up sequence:
    //      Proxy               Data Node               Control Client
    // 1                                                send REMOVE NODE
    // 2    receive
    // 3    send STOP
    // 4                        receive
    // 7    send REMOVE NODE
    //          to self
    // 8    load balancing
    //          & propagate table
    // 9    send UPDATE to nodes
    // 10                       receive
    private void onFollowupRemoveNode(String followupAddress, Response response) {
        Request request = (Request) response.getAttachment();
        send(request.getReceiver(), request, this);
        if (followupAddress == null) {
            SimpleLog.i("Could not find the address to follow up");
            return;
        }
        Request followRequest = daemon.getDataNodeServer()
                .getDataNode()
                .prepareRemoveNodeCommand(followupAddress);
        processDataNodeCommand(followRequest);
    }

    private void onFollowupFetch(String followupAddress, Response response) {
        if (followupAddress == null) {
            SimpleLog.i("Could not find the address to follow up");
            return;
        }
        Request request = daemon.getDataNodeServer()
                .getDataNode()
                .prepareAddNodeCommand(followupAddress);
        processDataNodeCommand(request);
    }

    @Override
    public void onInitialized() {
        // start nodes in table when initialization is done
        Request request = new Request()
                .withHeader(DaemonCommand.START.name());

        for (PhysicalNode node : daemon.getDataNodeServer().getPhysicalNodes()) {
            send(node.getAddress(), node.getPort(), request, this);
        }
    }

    @Override
    public void onRequestAvailable(List<Request> requests) {
        for (Request request : requests) {
            processDataNodeCommand(request);
            StatInfoManager.getInstance().statExecution(request, request.getTimestamp()); // stat load balancing event
        }
    }
}
