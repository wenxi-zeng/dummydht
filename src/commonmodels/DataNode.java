package commonmodels;

import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import util.Config;
import util.SimpleLog;

import java.util.List;

public abstract class DataNode {

    protected Terminal terminal;

    protected String ip;

    protected int port;

    protected List<String> seeds;

    protected boolean useDynamicAddress;

    protected String clusterName;

    protected String mode;

    protected BoundedQueue<Request> delta;

    protected TableChangedHandler tableChangedHandler;

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
        delta = new BoundedQueue<>(config.getTableDeltaSize());
    }

    public void createTable() {
        terminal.initialize();
    }

    public Response execute(String command) throws InvalidRequestException {
        return execute(terminal.translate(command));
    }

    public Response execute(String[] args) throws InvalidRequestException {
        return execute(terminal.translate(args));
    }

    public Response execute(Request request) {
        if (terminal.isRequestCauseTableUpdates(request)) {
            recordTableDelta(request);
        }

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

    public void recordTableDelta(Request request) {
        SimpleLog.i("recordTableDelta: " + request.toCommand() + " =========================");
        delta.add(request);
        if (tableChangedHandler != null)
            tableChangedHandler.onTableChanged(request, getTable());
    }

    public List<Request> getTableDelta() {
        return delta.toList();
    }

    public LoadChangeHandler getLoadChangeHandler() {
        return new LoadChangeHandlerFactory().getHandler(mode, Config.getInstance().getScheme(), getTable());
    }

    public TableChangedHandler getTableChangedHandler() {
        return tableChangedHandler;
    }

    public void setTableChangedHandler(TableChangedHandler tableChangedHandler) {
        this.tableChangedHandler = tableChangedHandler;
    }

    public abstract void createTerminal();
    public abstract Object getTable();
    public abstract long getEpoch();
    public abstract String createTable(Object o); // use for bootstrapping
    public abstract String updateTable(Object o);
    public abstract List<PhysicalNode> getPhysicalNodes();
    public abstract Request prepareListPhysicalNodesCommand();
    public abstract Request prepareAddNodeCommand();
    public abstract Request prepareAddNodeCommand(String nodeIp, int nodePort);
    public abstract Request prepareRemoveNodeCommand(String nodeIp, int nodePort);
    public abstract Request prepareLoadBalancingCommand(String... addresses);
    public abstract Request prepareIncreaseLoadCommand(String... addresses);
    public abstract Request prepareDecreaseLoadCommand(String... addresses);
    public abstract void setMembershipCallBack(MembershipCallBack callBack);
    public abstract void setReadWriteCallBack(ReadWriteCallBack callBack);
    public abstract void initTableDeltaSupplier();
}
