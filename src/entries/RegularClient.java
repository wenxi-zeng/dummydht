package entries;

import socket.SocketClient;
import util.SimpleLog;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class RegularClient {
    public static void main(String args[]) {
        //args = new String[] {"6000"};
        if (args.length != 1)
        {
            System.out.println ("Usage: RegularClient [ip:]<port>");
            return;
        }

        String[] params = args[0].split(":");

        InetSocketAddress address;
        if (params.length == 2) {
            address = new InetSocketAddress(params[0], Integer.valueOf(params[1]));
        }
        else if (params.length == 1) {
            address = new InetSocketAddress("localhost", Integer.valueOf(params[0]));
        }
        else {
            System.out.println ("Usage: RegularClient [ip:]<port>");
            return;
        }

        SocketClient socketClient = new SocketClient();
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Object o) {
                SimpleLog.v(String.valueOf(o));
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.v(error);
            }
        };

        Scanner in = new Scanner(System.in);
        String command = in.nextLine();

        while (!command.equalsIgnoreCase("exit")){
            socketClient.send(address, command, callBack);
            command = in.nextLine();
        }
    }
}
