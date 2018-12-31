package entries;

import ceph.CephCommand;
import elastic.ElasticCommand;
import ring.RingCommand;
import socket.SocketClient;
import util.SimpleLog;

import java.util.Scanner;

public class DataNodeTool {

    private SocketClient socketClient = new SocketClient();

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

    public static void main(String[] args){
        DataNodeTool dataNodeTool = new DataNodeTool();

        if (args.length == 0){
            Scanner in = new Scanner(System.in);
            String command = in.nextLine();

            while (!command.equalsIgnoreCase("exit")){
                dataNodeTool.process(command.split(","));
                command = in.nextLine();
            }
        }
        else {
            dataNodeTool.process(args);
        }
    }

    private void process(String[] args) {
        if (args[0].equalsIgnoreCase("help")) {
            SimpleLog.v(getHelp());
            return;
        }

        if (args[0].contains(":")) {
            String[] address = args[0].split(":");
            socketClient.send(address[0], Integer.valueOf(address[1]), getCommand(args, args[0]), callBack);
        }
        else {
            socketClient.send(Integer.valueOf(args[0]), getCommand(args, null), callBack);
        }
    }

    private static String getHelp() {
        return "Available commands:\n" +
                "[address:]<port> start <dht-type>\n" +
                "[address:]<port> stop\n" +
                "[address:]<port> <status | printLookupTable | listPhysicalNodes>\n\n" +
                "Ring commands:\n" +
                "[address:]<port>" + RingCommand.INCREASELOAD.getHelpString() + "\n" +
                "[address:]<port>" + RingCommand.DECREASELOAD.getHelpString() + "\n\n" +
                "Elastic commands:\n" +
                "[address:]<port>" + ElasticCommand.MOVEBUCKET.getHelpString() + "\n" +
                "[address:]<port>" + ElasticCommand.EXPAND.getHelpString() + "\n" +
                "[address:]<port>" + ElasticCommand.SHRINK.getHelpString() + "\n\n" +
                "Ceph commands:\n" +
                "[address:]<port>" + CephCommand.CHANGEWEIGHT.getHelpString() + "\n";
    }

    private static String getCommand(String[] args, String exclude) {
        StringBuilder stringBuilder = new StringBuilder();

        for (String arg : args) {
            if (arg.equals(exclude)) continue;
            stringBuilder.append(arg).append(" ");
        }

        return stringBuilder.toString().trim();
    }
}
