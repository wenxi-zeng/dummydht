package entries;

import commonmodels.PhysicalNode;
import datanode.DataNodeServer;
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
            DataNodeDaemon daemon = new DataNodeDaemon(getAddress(), daemonPort);
            SimpleLog.with(daemon.ip, daemon.port);
            SimpleLog.i("Daemon: " + daemonPort + " started");
            daemon.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DataNodeDaemon(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.socketServer = new SocketServer(this.port, this);
    }

    private void exec() throws Exception {
        socketServer.start();
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, String message) throws Exception {
        String cmdLine[] = message.split("\\s+");
        ByteBuffer buffer;

        if (dataNodeServer == null) {
            if (cmdLine[0].equals("start")) {
                //dataNodeServer = new DataNodeServer(cmdLine[2], getAddress(), Integer.valueOf(cmdLine[1]));
                dataNodeServer = new DataNodeServer(ip, port);
                dataNodeServer.start();
                FileTransferManager.getInstance().subscribe(this);
                buffer = ObjectConverter.getByteBuffer("Node started");
            }
            else {
                buffer = ObjectConverter.getByteBuffer("Datanode server is not started");
            }

            out.write(buffer).get();
        }
        else if (cmdLine[0].equals("stop")){
            dataNodeServer.stop();
            dataNodeServer = null;
            FileTransferManager.getInstance().unsubscribe(this);
            buffer = ObjectConverter.getByteBuffer("Node stopped");
            out.write(buffer).get();
        }
        else if (cmdLine[0].equals("status")) {
            buffer = ObjectConverter.getByteBuffer(dataNodeServer.getMembersStatus());
            out.write(buffer).get();
        }
        else if (cmdLine[0].equals("fetch")) {
            buffer = ObjectConverter.getByteBuffer(dataNodeServer.getDataNodeTable());
            out.write(buffer).get();
        }
        else if (cmdLine[0].equals("transfer")) {
            buffer = ObjectConverter.getByteBuffer("Received file transfer request.");
            out.write(buffer).get();
            dataNodeServer.transferFile(message);
        }
        else if (cmdLine[0].equals("copy")) {
            buffer = ObjectConverter.getByteBuffer("Received file replicate request.");
            out.write(buffer).get();
            dataNodeServer.copyFile(message);
        }
        else {
            buffer = ObjectConverter.getByteBuffer(dataNodeServer.processCommand(cmdLine));
            out.write(buffer).get();
        }
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, Object o) throws Exception {
        String result = dataNodeServer.updateTable(o);
        ByteBuffer buffer = ObjectConverter.getByteBuffer(result);
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

    private static String getAddress() {
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (UnknownHostException | SocketException e) {
            e.printStackTrace();
        }

        return null;
    }
}
