package datanode;

import ceph.CephDataNode;
import commonmodels.DataNode;
import commonmodels.PhysicalNode;
import datanode.strategies.CentralizedStrategy;
import datanode.strategies.DistributedStrategy;
import datanode.strategies.MembershipStrategy;
import elastic.ElasticDataNode;
import filemanagement.FileTransferManager;
import ring.RingDataNode;
import util.Config;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataNodeServer {

    private DataNode dataNode;

    private MembershipStrategy membershipStrategy;

    public DataNodeServer(String type, String ip, int port) throws Exception {
        initDataNode(type, ip, port);
        initStrategy();
    }

    private void initDataNode(String type, String ip, int port) throws Exception {
        if (type.equalsIgnoreCase("ring"))
            dataNode = new RingDataNode();
        else if (type.equalsIgnoreCase("elastic"))
            dataNode = new ElasticDataNode();
        else if (type.equalsIgnoreCase("ceph"))
            dataNode = new CephDataNode();
        else
            throw new Exception("Invalid DHT type");

        dataNode.setPort(port);
        dataNode.setIp(ip);
        dataNode.setUseDynamicAddress(true);
    }

    private void initStrategy() {
        if (dataNode.getMode().equals(Config.MODE_DISTRIBUTED)) {
            membershipStrategy = new DistributedStrategy(dataNode);
        }
        else {
            membershipStrategy = new CentralizedStrategy(dataNode);
        }
    }

    public void start() throws Exception {
        membershipStrategy.onNodeStarted();
    }

    public void stop() {
        membershipStrategy.onNodeStopped();
        dataNode.destroy();
    }

    public String getMembersStatus() {
        return membershipStrategy.getMembersStatus();
    }

    public Object getDataNodeTable() {
        return dataNode.getTable();
    }

    public String updateTable(Object o) {
        return dataNode.updateTable(o);
    }

    public void transferFile(String command) {
        String[] params = command.split(" ");

        Pattern pattern = Pattern.compile(",");
        List<Integer> buckets = pattern.splitAsStream(params[2])
                .map(Integer::valueOf)
                .collect(Collectors.toList());

        FileTransferManager.getInstance().transfer(
                buckets,
                new PhysicalNode(dataNode.getIp(), dataNode.getPort()),
                new PhysicalNode(params[1])
        );
    }

    public void copyFile(String command) {
        String[] params = command.split(" ");

        Pattern pattern = Pattern.compile(",");
        List<Integer> buckets = pattern.splitAsStream(params[2])
                .map(Integer::valueOf)
                .collect(Collectors.toList());

        FileTransferManager.getInstance().copy(
                buckets,
                new PhysicalNode(dataNode.getIp(), dataNode.getPort()),
                new PhysicalNode(params[1])
        );
    }

    public String processCommand(String[] args) {
        return dataNode.getTerminal().execute(args);
    }

}
