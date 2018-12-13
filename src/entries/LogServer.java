package entries;

import socket.SocketServer;
import util.SimpleLog;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

public class LogServer implements SocketServer.EventHandler {

    private SocketServer socketServer;

    public static void main(String[] args){
        if (args.length > 1)
        {
            System.err.println ("Usage: LogServer [daemon port]");
            return;
        }

        int daemonPort = 5999;
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
            LogServer demon = new LogServer(daemonPort);
            demon.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public LogServer(int port) {
        socketServer = new SocketServer(port, this);
    }

    private void exec() throws Exception {
        socketServer.start();
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, String message) {
        try {
            SimpleLog.v("[" + out.getRemoteAddress() + "]: " + message);
        } catch (IOException e) {
            SimpleLog.v("[NIO error]: " + message);
        }
    }
}
