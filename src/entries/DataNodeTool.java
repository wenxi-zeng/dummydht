package entries;

import ceph.CephCommand;
import commonmodels.transport.Response;
import elastic.ElasticCommand;
import org.apache.commons.lang3.StringUtils;
import ring.RingCommand;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.util.Scanner;

public class DataNodeTool {

    private SocketClient socketClient = new SocketClient();

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {

        @Override
        public void onResponse(Response o) {
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

        Config config = Config.getInstance();
        if (config.getMode().equals(Config.MODE_DISTRIBUTED)) {
            if (args[1].contains(":")) {
                String command = StringUtils.join(args,  ' ')
                        .replace("addNode",  "start")
                        .replace("removeNode", "stop");
                socketClient.send(args[1], command, callBack);
            }
            else {
                SimpleLog.v("Invalid command!");
            }
        }
        else if (config.getSeeds().size() > 0){
            socketClient.send(config.getSeeds().get(0), StringUtils.join(args, ' '), callBack);
        }
    }

    private static String getHelp() {
        switch (Config.getInstance().getScheme()) {
            case Config.SCHEME_RING:
                return getRingHelp();
            case Config.SCHEME_ELASTIC:
                return getElasticHelp();
            case Config.SCHEME_CEPH:
                return getCephHelp();
            default:
                return "Invalid scheme";
        }
    }

    private static String getRingHelp() {
        String help = RingCommand.ADDNODE.getHelpString() + "\n" +
                RingCommand.REMOVENODE.getHelpString() + "\n" +
                RingCommand.INCREASELOAD.getHelpString() + "\n" +
                RingCommand.DECREASELOAD.getHelpString() + "\n";

        if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED)) {
            help += RingCommand.LISTPHYSICALNODES.getHelpString() + " <ip>:<port>\n" +
                    RingCommand.PRINTLOOKUPTABLE.getHelpString() + " <ip>:<port>\n";
        }
        else {
            help += RingCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                    RingCommand.PRINTLOOKUPTABLE.getHelpString() + "\n";
        }

        return help;
    }

    private static String getElasticHelp() {
        String help = ElasticCommand.ADDNODE.getHelpString() + "\n" +
                        ElasticCommand.REMOVENODE.getHelpString() + "\n" +
                        ElasticCommand.MOVEBUCKET.getHelpString() + "\n";

        if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED)) {
            help += ElasticCommand.EXPAND.getHelpString() + " <ip>:<port>\n" +
                    ElasticCommand.SHRINK.getHelpString() + " <ip>:<port>\n" +
                    ElasticCommand.LISTPHYSICALNODES.getHelpString() + " <ip>:<port>\n" +
                    ElasticCommand.PRINTLOOKUPTABLE.getHelpString() + " <ip>:<port>\n";
        }
        else {
            help += ElasticCommand.EXPAND.getHelpString() + "\n" +
                    ElasticCommand.SHRINK.getHelpString() + "\n" +
                    ElasticCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                    ElasticCommand.PRINTLOOKUPTABLE.getHelpString() + "\n";
        }

        return help;
    }

    private static String getCephHelp() {
        String help = CephCommand.ADDNODE.getHelpString() + "\n" +
                        CephCommand.REMOVENODE.getHelpString() + "\n" +
                        CephCommand.CHANGEWEIGHT.getHelpString() + "\n" +
                        CephCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                        CephCommand.PRINTCLUSTERMAP.getHelpString() + "\n";

        if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED)) {
            help += CephCommand.LISTPHYSICALNODES.getHelpString() + " <ip>:<port>\n" +
                    CephCommand.PRINTCLUSTERMAP.getHelpString() + " <ip>:<port>\n";
        }
        else {
            help += CephCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                    CephCommand.PRINTCLUSTERMAP.getHelpString() + "\n";
        }

        return help;
    }
}
