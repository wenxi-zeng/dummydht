package entries;

import datanode.DataNodeServer;
import socket.SocketServer;
import util.ObjectConverter;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

public class DataNodeDaemon implements SocketServer.EventHandler {

    private SocketServer socketServer;

    private DataNodeServer dataNodeServer;

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
            DataNodeDaemon demon = new DataNodeDaemon(daemonPort);
            demon.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DataNodeDaemon(int port) {
        socketServer = new SocketServer(port, this);
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
                dataNodeServer = new DataNodeServer(cmdLine[2], Integer.valueOf(cmdLine[1]));
                dataNodeServer.start();
                buffer = ObjectConverter.getByteBuffer("Node started");
            }
            else {
                buffer = ObjectConverter.getByteBuffer("Datanode server is not started");
            }
        }
        else if (cmdLine[0].equals("stop")){
            dataNodeServer.stop();
            dataNodeServer = null;
            buffer = ObjectConverter.getByteBuffer("Node stopped");
        }
        else if (cmdLine[0].equals("status")) {
            buffer = ObjectConverter.getByteBuffer(dataNodeServer.getMembersStatus());
        }
        else {
            buffer = ObjectConverter.getByteBuffer(dataNodeServer.processCommand(cmdLine));
        }

        out.write(buffer).get();
    }
}
