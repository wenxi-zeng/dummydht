package entries;

import socket.UDPServer;
import util.Config;
import util.SimpleLog;

import java.net.SocketException;

public class UDPLogServer implements UDPServer.EventHandler {
    private UDPServer socketServer;

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
            UDPLogServer demon = new UDPLogServer(daemonPort);
            demon.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public UDPLogServer(int port) throws SocketException {
        socketServer = new UDPServer(port, this);
    }

    private void exec() throws Exception {
        socketServer.start();
    }

    @Override
    public void onReceived(Object o) {
        SimpleLog.v(String.valueOf(o));
    }
}
