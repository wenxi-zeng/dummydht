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
import req.Rand.ExpGenerator;
import req.Rand.RandomGenerator;
import req.Rand.UniformGenerator;
import req.Rand.ZipfGenerator;
import req.RequestTypeGenerator;
import req.StaticTree;
import ring.RingTerminal;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        RegularClient regularClient;

        try {
            regularClient = new RegularClient();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        if (args.length == 0) {
            regularClient.run();
        }
        else if (args[0].equals("-r")) {
            if (args.length == 2) {
                regularClient.generateRequest(args[1]);
            }
            else {
                System.out.println ("Usage: RegularClient -r <filename>");
            }
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

    private void generateRequest(String filename) {
        try {
            StaticTree tree = StaticTree.getStaticTree(filename);
            RequestGenerator generator = new RequestGenerator(tree, terminal);
            long interArrivalTime = Config.getInstance().getReadWriteInterArrivalTime();

            while(true) {
                Request request = generator.next();
                PhysicalNode server = choseServer(request.getAttachment());
                socketClient.send(server.getFullAddress(), request, callBack);

                Thread.sleep(interArrivalTime);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
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

    private PhysicalNode choseServer(String attachment) {
        String[] filename = attachment.split(" ");
        Request request = new Request()
                    .withHeader(RingCommand.LOOKUP.name())
                    .withAttachment(filename[0]);
        Response response = terminal.process(request);

        @SuppressWarnings("unchecked")
        List<PhysicalNode> pnodes = (List<PhysicalNode>) response.getAttachment();
        return pnodes.get(0);
    }

    public class RequestGenerator {

        private final RandomGenerator generator;

        private final StaticTree tree;

        private final Terminal terminal; // we need terminal in order to tag the request with epoch

        private RequestTypeGenerator headerGenerator;

        public RequestGenerator(StaticTree tree, Terminal terminal) {
            this.generator = loadGenerator(tree.getFileSize() - 1);
            this.tree = tree;
            this.terminal = terminal;

            Map<Request, Double> requestTypes = loadRequestRatio();
            this.headerGenerator = new RequestTypeGenerator(
                    loadRequestRatio(),
                    loadGenerator(requestTypes.size()));
        }

        public Request next() {
            Request header = headerGenerator.next();
            if (header.getHeader().equals(RingCommand.READ.name()))
                return nextRead();
            else
                return nextWrite();
        }

        public Request nextRead() {
            StaticTree.RandTreeNode file = tree.getFiles().get(generator.nextInt());
            String[] args = new String[] { RingCommand.READ.name(),  file.toString() };
            Request request = null;
            try {
                request = terminal.translate(args);
                request.setEpoch(terminal.getEpoch());
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }

            return request;
        }

        public Request nextWrite() {
            StaticTree.RandTreeNode file = tree.getFiles().get(generator.nextInt());
            String[] args = new String[] { RingCommand.WRITE.name(),  file.toString(), String.valueOf(file.getSize()) };
            Request request = null;
            try {
                request = terminal.translate(args);
                request.setEpoch(terminal.getEpoch());
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }

            return request;
        }

        private Map<Request, Double> loadRequestRatio() {
            double[] ratio = Config.getInstance().getReadWriteRatio();
            Map<Request, Double> map = new HashMap<>();
            map.put(new Request().withHeader(RingCommand.READ.name()), ratio[Config.RATIO_KEY_READ]);
            map.put(new Request().withHeader(RingCommand.WRITE.name()), ratio[Config.RATIO_KEY_WRITE]);
            return map;
        }

        private RandomGenerator loadGenerator(int upper) {
            UniformGenerator generator = new UniformGenerator(upper);

            if (Config.getInstance().getRequestDistribution().equals(Config.REQUEST_DISTRIBUTION_EXP))
                return new ExpGenerator(
                        Config.getInstance().getExpLamda(),
                        upper,
                        generator);
            else if (Config.getInstance().getRequestDistribution().equals(Config.REQUEST_DISTRIBUTION_ZIPF))
                return new ZipfGenerator(
                        Config.getInstance().getZipfAlpha(),
                        upper,
                        generator);
            else
                return generator;
        }
    }
}
