package entries;

import ceph.ClusterMap;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class RegularClient {

    private SocketClient socketClient = new SocketClient();

    private ClusterMap map;

    private elastic.LookupTable elasticTable;

    private ring.LookupTable ringTable;

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
        @Override
        public void onResponse(Object o) {
            SimpleLog.v(String.valueOf(o));


        }

        @Override
        public void onFailure(String error) {
            SimpleLog.v(error);
        }
    };

    public static void main(String args[]) {
        if (args.length > 1) return;

        RegularClient regularClient = new RegularClient();

        if (args.length == 0) {

        }
        else {
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

            regularClient.connect(address);
        }
    }

    private void connect(InetSocketAddress address) {
        Scanner in = new Scanner(System.in);
        String command = in.nextLine();

        while (!command.equalsIgnoreCase("exit")){
            socketClient.send(address, command, callBack);
            command = in.nextLine();
        }
    }

    private void exec() {
        SimpleLog.v("Fetching table...");

        if (Config.getInstance().getSeeds().size() > 0) {
            socketClient.send(Config.getInstance().getSeeds().get(0), "fetch", callBack);
        }
        else {
            SimpleLog.v("No seed/proxy info found!");
        }
    }
}
