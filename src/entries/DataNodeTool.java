package entries;

import ceph.CephTerminal;
import commands.CephCommand;
import commands.DaemonCommand;
import commands.ElasticCommand;
import commands.RingCommand;
import commonmodels.Terminal;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.ElasticTerminal;
import org.apache.commons.lang3.StringUtils;
import ring.RingTerminal;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.util.Scanner;

public class DataNodeTool {

    private Terminal terminal;

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
        DataNodeTool dataNodeTool;

        try{
            dataNodeTool = new DataNodeTool();

            if (args.length == 0){
                Scanner in = new Scanner(System.in);
                String command = in.nextLine();

                while (!command.equalsIgnoreCase("exit")){
                    dataNodeTool.process(command);
                    command = in.nextLine();
                }
            }
            else {
                dataNodeTool.process(StringUtils.join(args,  ' '));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DataNodeTool() throws Exception {
        String scheme = Config.getInstance().getScheme();

        switch (scheme) {
            case Config.SCHEME_RING:
                terminal = new RingTerminal();
                break;
            case Config.SCHEME_ELASTIC:
                terminal = new ElasticTerminal();
                break;
            case Config.SCHEME_CEPH:
                terminal = new CephTerminal();
                break;
            default:
                throw new Exception("Invalid DHT type");
        }
    }

    private void process(String command) throws Exception {
        if (command.equalsIgnoreCase("help")) {
            SimpleLog.v(getHelp());
            return;
        }

        Request request = terminal.translate(command);
        terminal.process(request);

        Config config = Config.getInstance();
        if (config.getMode().equals(Config.MODE_CENTRIALIZED)) {
            if (config.getSeeds().size() > 0) {
                request.setReceiver(config.getSeeds().get(0));
            }
            else {
                throw new Exception("Proxy not specified.");
            }
        }
        else {
            if (request.getHeader().equals(RingCommand.ADDNODE.name())) {
                request.setHeader(DaemonCommand.START.name());
            }
            else if (request.getHeader().equals(RingCommand.REMOVENODE.name())) {
                request.setHeader(DaemonCommand.STOP.name());
            }
        }

        socketClient.send(request.getReceiver(), request, callBack);
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
        return RingCommand.ADDNODE.getHelpString() + "\n" +
                RingCommand.REMOVENODE.getHelpString() + "\n" +
                RingCommand.INCREASELOAD.getHelpString() + "\n" +
                RingCommand.DECREASELOAD.getHelpString() + "\n" +
                RingCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                RingCommand.PRINTLOOKUPTABLE.getHelpString() + "\n";
    }

    private static String getElasticHelp() {
        return ElasticCommand.ADDNODE.getHelpString() + "\n" +
                        ElasticCommand.REMOVENODE.getHelpString() + "\n" +
                        ElasticCommand.MOVEBUCKET.getHelpString() + "\n" +
                        ElasticCommand.EXPAND.getHelpString() + "\n" +
                        ElasticCommand.SHRINK.getHelpString() + "\n" +
                        ElasticCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                        ElasticCommand.PRINTLOOKUPTABLE.getHelpString() + "\n";
    }

    private static String getCephHelp() {
        return CephCommand.ADDNODE.getHelpString() + "\n" +
                        CephCommand.REMOVENODE.getHelpString() + "\n" +
                        CephCommand.CHANGEWEIGHT.getHelpString() + "\n" +
                        CephCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                        CephCommand.PRINTCLUSTERMAP.getHelpString() + "\n";
    }
}
