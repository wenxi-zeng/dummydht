package entries;

import socket.UDPServer;
import util.Config;
import util.SimpleLog;

import java.io.IOException;
import java.net.SocketException;

public class StatServer implements UDPServer.EventHandler {

    public static void main(String[] args){
        if (args.length > 1)
        {
            System.err.println ("Usage: LogServer [daemon port]");
            return;
        }

        int daemonPort = Integer.valueOf(Config.getInstance().getStatServer().split(":")[1]);
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
            new StatServer(daemonPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public StatServer(int port) throws IOException {
        UDPServer socketServer = new UDPServer(port, this);
        new Thread(socketServer).start();
    }

    @Override
    public void onReceived(Object o) {
        SimpleLog.v(String.valueOf(o));
    }
}