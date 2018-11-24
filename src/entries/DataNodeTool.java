package entries;

import socket.SocketClient;
import util.SimpleLog;

public class DataNodeTool {

    public static void main(String[] args){
        SocketClient socketClient = new SocketClient();
        SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
            @Override
            public void onResponse(Object o) {
                SimpleLog.i(String.valueOf(o));
            }

            @Override
            public void onFailure(String error) {
                SimpleLog.i(error);
            }
        };

        args = "start 6000 ring".split(" ");
        if (args.length < 2 || args.length > 4) {
            SimpleLog.i("Wrong arguments. Available commands:\n" +
                    "start [address] <port> <dht-type>\n" +
                    "stop [address] <port>\n" +
                    "status [address] <port>");
        }

        if (args[1].matches("\\d+")) {
            socketClient.send(Integer.valueOf(args[1]), getCommand(args, null), callBack);
        }
        else {
            socketClient.send(args[1], Integer.valueOf(args[2]), getCommand(args, args[1]), callBack);
        }
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
