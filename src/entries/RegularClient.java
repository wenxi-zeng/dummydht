package entries;

import ceph.CephTerminal;
import commands.DaemonCommand;
import commands.RingCommand;
import commonmodels.PhysicalNode;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.ElasticTerminal;
import ring.RingTerminal;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Scanner;

public class RegularClient {

    private SocketClient socketClient = new SocketClient();

    private Terminal terminal;

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {
        @Override
        public void onResponse(Response o) {
            if (o.getHeader().equals(DaemonCommand.FETCH.name())) {
                onTableFetched(o.getAttachment());
            }
            SimpleLog.v(String.valueOf(o));
        }

        @Override
        public void onFailure(String error) {
            SimpleLog.v(error);
        }
    };

    public static void main(String args[]) {
        if (args.length > 1) return;

        RegularClient regularClient = null;

        try {
            regularClient = new RegularClient();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        if (args.length == 0) {
            regularClient.run();
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

    public RegularClient() throws Exception {
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

    private void connect(InetSocketAddress address) {
        Scanner in = new Scanner(System.in);
        String command = in.nextLine();

        while (!command.equalsIgnoreCase("exit")){
            try {
                Request request = terminal.translate(command);
                socketClient.send(address, request, callBack);
                command = in.nextLine();
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }
        }
    }

    private void connect() {
        Scanner in = new Scanner(System.in);
        String command = in.nextLine();

        while (!command.equalsIgnoreCase("exit")){
            try {
                Request request = terminal.translate(command);
                if (request.getHeader().equals(RingCommand.READ.name()) ||
                        request.getHeader().equals(RingCommand.WRITE.name())) {
                    request.withEpoch(terminal.getEpoch());
                    PhysicalNode server = choseServer(request.getAttachment());
                    socketClient.send(server.getFullAddress(), request, callBack);
                }
                else {
                    Response response = terminal.process(request);
                    SimpleLog.v(String.valueOf(response));
                }

                command = in.nextLine();
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }
        }
    }

    private void run() {
        SimpleLog.v("Fetching table...");

        if (Config.getInstance().getSeeds().size() > 0) {
            Request request = new Request().withHeader(DaemonCommand.FETCH.name());
            socketClient.send(Config.getInstance().getSeeds().get(0), request, callBack);
        }
        else {
            SimpleLog.v("No seed/proxy info found!");
        }
    }
    private void onTableFetched(Object table) {
        Request request = new Request()
                            .withHeader(DaemonCommand.UPDATE.name())
                            .withLargeAttachment(table);

        Response response = terminal.process(request);
        SimpleLog.v(String.valueOf(response));

        connect();
    }

    private PhysicalNode choseServer(String filename) {
        Request request = new Request()
                    .withHeader(RingCommand.LOOKUP.name())
                    .withAttachment(filename);
        Response response = terminal.process(request);

        @SuppressWarnings("unchecked")
        List<PhysicalNode> pnodes = (List<PhysicalNode>) response.getAttachment();
        return pnodes.get(0);
    }
}
