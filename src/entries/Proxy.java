package entries;

import ceph.CephDataNode;
import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import elastic.ElasticDataNode;
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

public class Proxy implements SocketServer.EventHandler, FileTransferManager.FileTransferRequestCallBack {

    private SocketServer socketServer;

    private SocketClient socketClient = new SocketClient();

    private DataNode dataNode;

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
        @Override
        public void onResponse(Object o) {
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
        this.socketServer = new SocketServer(dataNode.getPort(), this);
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
        else {
            buffer = ObjectConverter.getByteBuffer(dataNode.execute(message));
            out.write(buffer).get();
        }
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, Object o) throws Exception {
        ByteBuffer buffer = ObjectConverter.getByteBuffer("Proxy should not receive an object");
        out.write(buffer).get();
    }

    @Override
    public void onFileTransfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        String request = formatRequest("transfer", buckets, toNode);
        socketClient.send(from.getAddress(), from.getPort(), request, callBack);
    }

    @Override
    public void onFileReplicate(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        String request = formatRequest("copy", buckets, toNode);
        socketClient.send(from.getAddress(), from.getPort(), request, callBack);
    }

    private String formatRequest(String header, List<Integer> buckets, PhysicalNode dest) {
        return String.format("%s %s:%s %s",
                header,
                dest.getAddress(),
                dest.getPort(),
                StringUtils.join(buckets, ','));
    }

    private void propagateTable() {
        for (PhysicalNode node : dataNode.getPhysicalNodes()) {
            socketClient.send(node.getAddress(), node.getPort(), dataNode.getTable(), callBack);
        }
    }

    private void addNode(String address, int port) {
        dataNode.execute(dataNode.prepareAddNodeCommand(address, port));
    }
}
