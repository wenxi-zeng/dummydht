package commonmodels;

import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import util.Config;

import java.util.List;

public abstract class DataNode {

    protected Terminal terminal;

    protected String ip;

    protected int port;

    protected List<String> seeds;

    protected boolean useDynamicAddress;

    protected String clusterName;

    protected String mode;

    public DataNode() {
        useDynamicAddress = false;
        loadProperties();
        createTerminal();
    }

    public DataNode(int port) {
        this.port = port;
        useDynamicAddress = true;
        loadProperties();
        createTerminal();
    }

    public DataNode(String ip, int port) {
        this.ip = ip;
        this.port = port;
        useDynamicAddress = false;
        loadProperties();
        createTerminal();
    }

    public void destroy() {
        if (terminal != null)
            terminal.destroy();
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public List<String> getSeeds() {
        return seeds;
    }

    public Terminal getTerminal() {
        return terminal;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getMode() {
        return mode;
    }

    public String getAddress() {
        return ip == null ?  null : ip + ":" + port;
    }

    public String getGossipId() {
        return ip + "." + port;
    }

    public String getLocalAddress() {
        return "127.0.0.1:" + port;
    }

    public void setUseDynamicAddress(boolean useDynamicAddress) {
        this.useDynamicAddress = useDynamicAddress;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    private void loadProperties() {
        Config config = Config.getInstance();
        seeds = config.getSeeds();
        clusterName = config.getClusterName();
        mode = config.getMode();
    }

    public void createTable() {
        terminal.initialize();
    }

    public Response execute(String command) throws InvalidRequestException {
        return execute(command.split("\\s+"));
    }

    public Response execute(String[] args) throws InvalidRequestException {
        return terminal.process(args);
    }

    public Response execute(Request request) {
        return terminal.process(request);
    }

    public Request prepareAddNodeCommand(String address) {
        String[] addressStr = address.split(":");
        return prepareAddNodeCommand(addressStr[0], Integer.valueOf(addressStr[1]));
    }

    public Request prepareRemoveNodeCommand(String address) {
        String[] addressStr = address.split(":");
        return prepareRemoveNodeCommand(addressStr[0], Integer.valueOf(addressStr[1]));
    }

    public abstract void createTerminal();
    public abstract Object getTable();
    public abstract String updateTable(Object o);
    public abstract List<PhysicalNode> getPhysicalNodes();
    public abstract Request prepareListPhysicalNodesCommand();
    public abstract Request prepareAddNodeCommand();
    public abstract Request prepareAddNodeCommand(String nodeIp, int nodePort);
    public abstract Request prepareRemoveNodeCommand(String nodeIp, int nodePort);
    public abstract Request prepareLoadBalancingCommand(String... addresses);
    public abstract Request prepareIncreaseLoadCommand(String... addresses);
    public abstract Request prepareDecreaseLoadCommand(String... addresses);
    public abstract LoadChangeHandler getLoadChangeHandler();
    public abstract void setLoadBalancingCallBack(LoadBalancingCallBack callBack);
    public abstract void setMembershipCallBack(MembershipCallBack callBack);
    public abstract void setReadWriteCallBack(ReadWriteCallBack callBack);
}
