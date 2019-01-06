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
import req.Rand.RandomGenerator;
import ring.RingDataNode;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DataNodeTool {

    private DataNode dataNode;

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

    public class RequestGenerator {

        private final RandomGenerator generator;

        private List<PhysicalNode> activePhysicalNodes;

        private List<PhysicalNode> inactivePhysicalNodes;

        private DataNode dataNode;

        public RequestGenerator(RandomGenerator generator, List<PhysicalNode> physicalNodes, DataNode dataNode) {
            this.generator = generator;
            this.inactivePhysicalNodes = physicalNodes;
            this.activePhysicalNodes = new ArrayList<>();
            this.dataNode = dataNode;
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
    }
}
