package entries;

import ceph.CephDataNode;
import commands.CephCommand;
import commands.DaemonCommand;
import commands.ElasticCommand;
import commands.RingCommand;
import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.ElasticDataNode;
import org.apache.commons.lang3.StringUtils;
import req.RequestGenerator;
import req.RequestService;
import req.RequestThread;
import ring.RingDataNode;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class DataNodeTool {

    private DataNode dataNode;

    private SocketClient socketClient = new SocketClient();

    private CountDownLatch latch;

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {

        @Override
        public void onResponse(Response o) {
            if (o.getHeader().equals(DaemonCommand.FETCH.name())) {
                onTableFetched(o.getAttachment());
                latch.countDown();
            }
            SimpleLog.v(String.valueOf(o));
        }

        @Override
        public void onFailure(String error) {
            SimpleLog.v(error);
        }
    };

    private RequestThread.RequestGenerateThreadCallBack requestGenerateThreadCallBack = request -> {
        try {
            process(request.toCommand());
        } catch (Exception e) {
            e.printStackTrace();
        }
    };

    public static void main(String[] args){
        DataNodeTool dataNodeTool;

        try{
            SimpleLog.with("ControlClient", 1);
            dataNodeTool = new DataNodeTool();
            dataNodeTool.run();

            if (args.length == 0){
                Scanner in = new Scanner(System.in);
                String command = in.nextLine();

                while (!command.equalsIgnoreCase("exit")){
                    dataNodeTool.process(command);
                    command = in.nextLine();
                }
            }
            else if (args[0].equals("-r")) {
                if (args.length == 2) {
                    dataNodeTool.generateRequest();
                }
                else {
                    System.out.println ("Usage: RegularClient -r <filename>");
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
                dataNode = new RingDataNode();
                break;
            case Config.SCHEME_ELASTIC:
                dataNode = new ElasticDataNode();
                break;
            case Config.SCHEME_CEPH:
                dataNode = new CephDataNode();
                break;
            default:
                throw new Exception("Invalid DHT type");
        }
        dataNode.createTerminal();
        dataNode.getTerminal().initialize();
    }

    private void run() throws InterruptedException {
        SimpleLog.v("Fetching table...");

        if (Config.getInstance().getSeeds().size() > 0) {
            Request request = new Request().withHeader(DaemonCommand.FETCH.name());
            socketClient.send(Config.getInstance().getSeeds().get(0), request, callBack);
            latch = new CountDownLatch(1);
            latch.await();
        }
        else {
            SimpleLog.v("No seed/proxy info found!");
        }
    }

    private void onTableFetched(Object table) {
        Request request = new Request()
                .withHeader(DaemonCommand.UPDATE.name())
                .withLargeAttachment(table);

        Response response = dataNode.execute(request);
        SimpleLog.v(String.valueOf(response));
    }

    private void generateRequest() {
        RequestGenerator generator = new ControlRequestGenerator(new ArrayList<>(), dataNode);
        int numThreads = Config.getInstance().getNumberOfThreads();
        RequestService service = new RequestService(numThreads,
                Config.getInstance().getLoadBalancingInterArrivalTime(),
                generator,
                requestGenerateThreadCallBack);

        try {
            service.start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void process(String command) throws Exception {
        if (command.equalsIgnoreCase("help")) {
            SimpleLog.v(getHelp());
            return;
        }

        Request request = dataNode.getTerminal().translate(command);
        dataNode.execute(request);

        Config config = Config.getInstance();
        if (config.getMode().equals(Config.MODE_CENTRIALIZED)) {
            if (config.getSeeds().size() > 0) {
                if (request.getReceiver() == null)
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

    public class ControlRequestGenerator extends RequestGenerator {

        private List<PhysicalNode> activePhysicalNodes;

        private List<PhysicalNode> inactivePhysicalNodes;

        private DataNode dataNode;

        public ControlRequestGenerator(List<PhysicalNode> physicalNodes, DataNode dataNode) {
            super(physicalNodes.size() - 1);
            this.inactivePhysicalNodes = physicalNodes;
            this.activePhysicalNodes = new ArrayList<>();
            this.dataNode = dataNode;
        }

        @Override
        public Request next() throws Exception {
            Request header = headerGenerator.next();
            if (header.getHeader().equals(CephCommand.ADDNODE.name()))
                return nextAdd();
            else if (header.getHeader().equals(CephCommand.REMOVENODE.name()))
                return nextRemove();
            else
                return nextLoadBalancing();
        }

        public Request nextAdd() throws Exception {
            if (inactivePhysicalNodes.size() < 1)
                throw new Exception("No node available for addition anymore.");

            int key = generator.nextInt(inactivePhysicalNodes.size() - 1);
            PhysicalNode node = inactivePhysicalNodes.get(key);
            activePhysicalNodes.add(node);
            inactivePhysicalNodes.remove(node);

            return dataNode.prepareAddNodeCommand(
                    node.getAddress(),
                    node.getPort());
        }

        public Request nextRemove() throws Exception {
            if (activePhysicalNodes.size() < 1)
                throw new Exception("No node available for removal anymore.");

            int key = generator.nextInt(activePhysicalNodes.size() - 1);
            PhysicalNode node = activePhysicalNodes.get(key);
            inactivePhysicalNodes.add(node);
            activePhysicalNodes.add(node);

            return dataNode.prepareRemoveNodeCommand(
                    node.getAddress(),
                    node.getPort());
        }

        public Request nextLoadBalancing() throws Exception {
            if (activePhysicalNodes.size() < 2)
                throw new Exception("Not enough active nodes for load balancing");

            int key1 = generator.nextInt(activePhysicalNodes.size() - 1);
            int key2;
            do {
                key2 = generator.nextInt(activePhysicalNodes.size() - 1);
            } while (key1 == key2);

            PhysicalNode node1 = activePhysicalNodes.get(key1);
            PhysicalNode node2 = activePhysicalNodes.get(key2);

            return dataNode.prepareLoadBalancingCommand(
                    node1.getAddress() + " " + node1.getPort(),
                    node2.getAddress() + " " + node2.getPort());
        }

        @Override
        public Map<Request, Double> loadRequestRatio() {
            double[] ratio = Config.getInstance().getReadWriteRatio();
            Map<Request, Double> map = new HashMap<>();
            map.put(new Request().withHeader(CephCommand.ADDNODE.name()), ratio[Config.RATIO_KEY_READ]);
            map.put(new Request().withHeader(CephCommand.REMOVENODE.name()), ratio[Config.RATIO_KEY_WRITE]);
            map.put(new Request().withHeader(CephCommand.CHANGEWEIGHT.name()), ratio[Config.RATIO_KEY_LOAD_BALANCING]);
            return map;
        }
    }
}
