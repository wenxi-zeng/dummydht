package entries;

import commands.DaemonCommand;
import commands.ProxyCommand;
import commonmodels.Daemon;
import commonmodels.LoadBalancingCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import datanode.DataNodeServer;
import filemanagement.FileBucket;
import socket.SocketClient;
import util.Config;
import util.ObjectConverter;
import util.SimpleLog;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Proxy implements Daemon, LoadBalancingCallBack {

    private DataNodeDaemon daemon;

    private ExecutorService pool = Executors.newFixedThreadPool(1);

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
    }

    @Override
    public void onFinished() {
        // TODO: propagate table when load balancing is done
        //propagateTable();
    }

    @Override
    public void exec() throws Exception {
        startDataNodeServer();
        daemon.exec();
    }

    @Override
    public void startDataNodeServer() throws Exception {
        daemon.startDataNodeServer();
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
    public void onReceived(AsynchronousSocketChannel out, Request o) throws Exception {
        Response response = processCommonCommand(o);
        if (response.getStatus() == Response.STATUS_INVALID_REQUEST)
            response = processDataNodeCommand(o);

        ByteBuffer buffer = ObjectConverter.getByteBuffer(response);
        out.write(buffer).get();
        startFollowupTask(o.getFollowup(), response);
    }

    @Override
    public void onResponse(Response o) {
        SimpleLog.i(String.valueOf(o));
    }

    @Override
    public void onFailure(String error) {
        SimpleLog.i(error);
    }

    private void startFollowupTask(String followupAddress, Response response) {
        pool.execute(() -> followup(followupAddress, response));
    }

    // follow up sequence:
    //      Proxy               Data Node               Control Client
    // 1                                                send ADD/REMOVE NODE
    // 2    receive
    // 3    send START/STOP
    // 4                        receive
    // 5                        send FETCH
    // 6    receive
    // 7    send ADD/REMOVE NODE
    //          to self
    // 8    send PROPAGATE
    //          to self
    // 9    send UPDATE to nodes
    // 10                       receive
    private void followup(String followupAddress, Response response) {
        if (response.getStatus() == Response.STATUS_FAILED) return;

        if (response.getHeader().equals(ProxyCommand.PROPAGATE.name())) {
            Request request = new Request()
                    .withHeader(DaemonCommand.UPDATE.name())
                    .withLargeAttachment(daemon.getDataNodeServer().getDataNodeTable());
            propagateTable(request);
        }
        else if (response.getHeader().equals(ProxyCommand.ADDNODE.name()) ||
                response.getHeader().equals(ProxyCommand.REMOVENODE.name())) {
            Request request = (Request) response.getAttachment();
            send(request.getReceiver(), request, this);
        }
        else if (response.getHeader().equals(ProxyCommand.FETCH.name())) {
            if (followupAddress == null) {
                SimpleLog.i("Could not find the address to follow up");
                return;
            }
            Request request = daemon.getDataNodeServer()
                                    .getDataNode()
                                    .prepareAddNodeCommand(followupAddress);
            processDataNodeCommand(request);
            followup(followupAddress, new Response().withHeader(ProxyCommand.PROPAGATE.name()));
        }
    }
}
