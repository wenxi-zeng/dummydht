package entries;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
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
        try {
            socketServer = new SocketServer(port, this);
            Thread t = new Thread(socketServer);
            t.setDaemon(true);
            t.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void exec() {
        socketServer.run();
    }

    @Override
    public Response onReceived(Request o) {
        SimpleLog.v("[" + o.getSender() + "]: " + o.toString());

        return null;
    }

    @Override
    public void onBound() {

    }
}
