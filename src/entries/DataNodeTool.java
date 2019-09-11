package entries;

import ceph.CephDataNode;
import commands.CephCommand;
import commands.CommonCommand;
import commands.ElasticCommand;
import commands.RingCommand;
import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
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
import util.URIHelper;

import java.util.*;

public class DataNodeTool {

    private DataNode dataNode;

    private SocketClient socketClient;

    private SocketClient.ServerCallBack callBack = new SocketClient.ServerCallBack() {

        @Override
        public void onResponse(Request request, Response o) {
            if (o.getHeader().equals(CommonCommand.FETCH.name())) {
                onTableFetched(o.getAttachment());
            }
            SimpleLog.v(String.valueOf(o));
        }

        @Override
        public void onFailure(Request request, String error) {
            SimpleLog.v(error);
        }
    };

    private RequestThread.RequestGenerateThreadCallBack requestGenerateThreadCallBack = (request, client) -> {
        try {
            process(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    };

    public static void main(String[] args){
        DataNodeTool dataNodeTool;

        try{
            SimpleLog.with("ControlClient", 1);
            dataNodeTool = new DataNodeTool();

            if (args.length == 0){
                dataNodeTool.run();
                Scanner in = new Scanner(System.in);
                String command = in.nextLine();

                while (!command.equalsIgnoreCase("exit")){
                    dataNodeTool.process(command);
                    command = in.nextLine();
                }
            }
            else if (args[0].equals("-r")) {
                if (args.length == 2) {
                    dataNodeTool.run();
                    dataNodeTool.generateRequest();
                }
                else {
                    System.out.println ("Usage: RegularClient -r <filename>");
                }
            }
            else {
                dataNodeTool.process(StringUtils.join(args,  ' '));
                // System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public DataNodeTool() throws Exception {
        socketClient = SocketClient.getInstance();
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
            Request request = new Request().withHeader(CommonCommand.FETCH.name());
            socketClient.send(Config.getInstance().getSeeds().get(0), request, callBack);
        }
        else {
            SimpleLog.v("No seed/proxy info found!");
        }
    }

    private void onTableFetched(Object table) {
        Request request = new Request()
                .withHeader(CommonCommand.UPDATE.name())
                .withLargeAttachment(table);

        Response response = dataNode.execute(request);
        SimpleLog.v(String.valueOf(response));
    }

    private void generateRequest() {
        RequestGenerator generator = new ControlRequestGenerator(new ArrayList<>(), dataNode);
        int numThreads = Config.getInstance().getNumberOfThreads();
        RequestService service = new RequestService(numThreads,
                Config.getInstance().getLoadBalancingInterArrivalTime(),
                -1,
                generator,
                requestGenerateThreadCallBack);

        service.start();
    }

    private void process(String command) throws Exception {
        if (command.equalsIgnoreCase("help")) {
            SimpleLog.v(getHelp());
            return;
        }

        Request request;
        try {
            request = dataNode.getTerminal().translate(command);
        }
        catch (InvalidRequestException ignored) {
            request = new CommonTerminal().translate(command);
        }

        process(request);
    }

    private void process(Request request) throws Exception {
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
                request.setHeader(CommonCommand.START.name());
            }
            else if (request.getHeader().equals(RingCommand.REMOVENODE.name())) {
                request.setHeader(CommonCommand.STOP.name());
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

    public class CommonTerminal implements Terminal {

        @Override
        public void initialize() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public void printInfo() {

        }

        @Override
        public long getEpoch() {
            return 0;
        }

        @Override
        public Response process(String[] args) throws InvalidRequestException {
            return null;
        }

        @Override
        public Response process(Request request) {
            return null;
        }

        @Override
        public Request translate(String[] args) throws InvalidRequestException {
            try {
                CommonCommand cmd = CommonCommand.valueOf(args[0].toUpperCase());
                URIHelper.verifyAddress(args);
                return cmd.convertToRequest(args);
            }
            catch (IllegalArgumentException e) {
                throw new InvalidRequestException("Command " + args[0] + " not found");
            }
        }

        @Override
        public Request translate(String command) throws InvalidRequestException {
            return translate(command.split(" "));
        }

        @Override
        public boolean isRequestCauseTableUpdates(Request request) {
            return false;
        }
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
