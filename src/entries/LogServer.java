package entries;

import commonmodels.transport.Request;
import socket.SocketServer;
import util.Config;
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

        int daemonPort = Integer.valueOf(Config.getInstance().getLogServer().split(":")[1]);
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
    public void onReceived(AsynchronousSocketChannel out, Request o, SocketServer.EventResponsor responsor) throws Exception {
        try {
            SimpleLog.v("[" + out.getRemoteAddress() + "]: " + o.toString());
        } catch (IOException e) {
            SimpleLog.v("[NIO error]: " + o);
        }
    }

    @Override
    public void onBound() {

    }
}
