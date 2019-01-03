package entries;

import commonmodels.CommonCommand;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import datanode.DataNodeServer;
import filemanagement.FileBucket;
import filemanagement.FileTransferManager;
import org.apache.commons.lang3.StringUtils;
import socket.SocketClient;
import socket.SocketServer;
import util.ObjectConverter;
import util.SimpleLog;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;

public class DataNodeDaemon implements SocketServer.EventHandler, FileTransferManager.FileTransferRequestCallBack {

    private SocketServer socketServer;

    private SocketClient socketClient = new SocketClient();

    private DataNodeServer dataNodeServer;

    private String ip;

    private int port;

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
            System.err.println ("Usage: DataNodeDaemon [daemon port]");
            return;
        }

        int daemonPort = 6000;
        if (args.length > 0)
        {
            try
            {
                daemonPort = Integer.parseInt(args[0]);
            }
            catch (NumberFormatException e)
            {
                System.err.println ("Invalid daemon port: " + e);
                return;
            }
            if (daemonPort <= 0 || daemonPort > 65535)
            {
                System.err.println ("Invalid daemon port");
                return;
            }
        }

        try {
            DataNodeDaemon daemon = DataNodeDaemon.newInstance(getAddress(), daemonPort);
            SimpleLog.with(daemon.ip, daemon.port);
            SimpleLog.i("Daemon: " + daemonPort + " started");
            daemon.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static volatile DataNodeDaemon instance = null;

    public static DataNodeDaemon getInstance() {
        return instance;
    }

    public static DataNodeDaemon newInstance(String ip, int port) {
        instance = new DataNodeDaemon(ip, port);
        return getInstance();
    }

    private DataNodeDaemon(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.socketServer = new SocketServer(this.port, this);
    }

    private void exec() throws Exception {
        socketServer.start();
    }

    public void startDataNodeServer() throws Exception {
        if (dataNodeServer == null) {
            dataNodeServer = new DataNodeServer(ip, port);
            dataNodeServer.start();
            FileTransferManager.getInstance().subscribe(this);
        }
        else {
            throw new Exception("Data node is already started");
        }
    }

    public void stopDataNodeServer() throws Exception {
        if (dataNodeServer == null) {
            throw new Exception("Data node is not started yet");
        }
        else {
            dataNodeServer.stop();
            dataNodeServer = null;
            FileTransferManager.getInstance().unsubscribe(this);
        }
    }

    public DataNodeServer getDataNodeServer() {
        return dataNodeServer;
    }

    private static String getAddress() {
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (UnknownHostException | SocketException e) {
            e.printStackTrace();
        }

        return null;
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
        try {
            return dataNodeServer.processCommand(o);
        }
        catch (InvalidRequestException e) {
            return new Response(o).withStatus(Response.STATUS_FAILED)
                    .withMessage(e.getMessage());
        }
    }
}
