package entries;

import ceph.CephDataNode;
import commonmodels.CommonCommand;
import commonmodels.DataNode;
import commonmodels.LoadBalancingCallBack;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.ElasticDataNode;
import filemanagement.FileBucket;
import filemanagement.FileTransferManager;
import org.apache.commons.lang3.StringUtils;
import ring.RingDataNode;
import socket.SocketClient;
import socket.SocketServer;
import util.ObjectConverter;
import util.SimpleLog;
import util.URIHelper;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;

public class Proxy implements SocketServer.EventHandler, FileTransferManager.FileTransferRequestCallBack, LoadBalancingCallBack {

    private SocketServer socketServer;

    private SocketClient socketClient = new SocketClient();

    private DataNode dataNode;

    private String type;

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
        @Override
        public void onResponse(Response o) {
            SimpleLog.i(String.valueOf(o));
        }

        @Override
        public void onFailure(String error) {
            SimpleLog.i(error);
        }
    };

    public static void main(String[] args){
        if (args.length > 1)
        {
            System.err.println ("Usage: Proxy [DHT type]");
            return;
        }

        String type = "ring";
        if (args.length > 0)
        {
            type = args[0];
        }

        try {
            Proxy proxy = new Proxy(type);
            SimpleLog.with(proxy.dataNode.getIp(), proxy.dataNode.getPort());
            SimpleLog.i("Proxy: started");
            proxy.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Proxy(String type) throws Exception {
        if (type.equalsIgnoreCase("ring"))
            dataNode = new RingDataNode();
        else if (type.equalsIgnoreCase("elastic"))
            dataNode = new ElasticDataNode();
        else if (type.equalsIgnoreCase("ceph"))
            dataNode = new CephDataNode();
        else
            throw new Exception("Invalid DHT type");

        if (dataNode.getSeeds() == null || dataNode.getSeeds().size() < 1)
            throw new Exception("Proxy not specified");

        URI uri = URIHelper.getGossipURI(dataNode.getSeeds().get(0));
        dataNode.setPort(uri.getPort());
        dataNode.setIp(uri.getHost());
        dataNode.setUseDynamicAddress(true);
        dataNode.setLoadBalancingCallBack(this);
        this.socketServer = new SocketServer(dataNode.getPort(), this);
        this.type = type;
        FileTransferManager.getInstance().subscribe(this);
    }

    private void exec() throws Exception {
        socketServer.start();
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, String message) throws Exception {
        String cmdLine[] = message.split("\\s+");
        ByteBuffer buffer;

        if (cmdLine[0].equals("status")) {
            buffer = ObjectConverter.getByteBuffer(
                    dataNode.execute(dataNode.prepareListPhysicalNodesCommand()));
            out.write(buffer).get();
        }
        else if (cmdLine[0].equals("fetch")) {
            buffer = ObjectConverter.getByteBuffer(dataNode.getTable());
            InetSocketAddress address = (InetSocketAddress)out.getRemoteAddress();
            String host = address.getHostName();
            int port = address.getPort();
            out.write(buffer).get();
            addNode(host, port);
        }
        else if (cmdLine[0].equals("propagate")) {
            buffer = ObjectConverter.getByteBuffer("Received propagation request.");
            out.write(buffer).get();
            propagateTable();
        }
        else if (cmdLine[0].equals("addNode")) {
            buffer = ObjectConverter.getByteBuffer(dataNode.execute(message));
            out.write(buffer).get();

            String request = "start " + type;
            socketClient.send(cmdLine[1], Integer.valueOf(cmdLine[2]), request, callBack);
        }
        else if (cmdLine[0].equals("removeNode")) {
            buffer = ObjectConverter.getByteBuffer(dataNode.execute(message));
            out.write(buffer).get();

            String request = "stop";
            socketClient.send(cmdLine[1], Integer.valueOf(cmdLine[2]), request, callBack);
        }
        else {
            buffer = ObjectConverter.getByteBuffer(dataNode.execute(message));
            out.write(buffer).get();
        }
    }

    private void propagateTable() {
        for (PhysicalNode node : dataNode.getPhysicalNodes()) {
            socketClient.send(node.getAddress(), node.getPort(), dataNode.getTable(), callBack);
        }
    }

    private void addNode(String address, int port) {
        dataNode.execute(dataNode.prepareAddNodeCommand(address, port));
    }

    @Override
    public void onFinished() {
        propagateTable();
    }


    @Override
    public void onTransferring(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        Request request = new Request()
                .withHeader(CommonCommand.TRANSFER.name())
                .withSender(from.getFullAddress())
                .withReceiver(toNode.getFullAddress())
                .withAttachment(StringUtils.join(buckets, ','));
        socketClient.send(from.getAddress(), from.getPort(), request, callBack);
    }

    @Override
    public void onReplicating(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        Request request = new Request()
                .withHeader(CommonCommand.COPY.name())
                .withSender(from.getFullAddress())
                .withReceiver(toNode.getFullAddress())
                .withAttachment(StringUtils.join(buckets, ','));
        socketClient.send(from.getAddress(), from.getPort(), request, callBack);
    }

    @Override
    public void onTransmitted(List<FileBucket> buckets, PhysicalNode from, PhysicalNode toNode) {

    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, Request o) throws Exception {
        Response response = processCommonCommand(o);
        if (response.getStatus() == Response.STATUS_FAILED)
            response = processDataNodeCommand(o);

        ByteBuffer buffer = ObjectConverter.getByteBuffer(response);
        out.write(buffer).get();
    }

    private Response processCommonCommand(Request o) {
        try {
            CommonCommand command = CommonCommand.valueOf(o.getHeader());
            return command.execute(o);
        }
        catch (IllegalArgumentException e) {
            return new Response(o).withStatus(Response.STATUS_FAILED)
                    .withMessage(e.getMessage());
        }
    }

    private Response processDataNodeCommand(Request o) {
        return dataNode.execute(o);
    }
}
