package commonmodels;

import util.Config;

import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static util.Config.*;

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
    }

    public DataNode(int port) {
        this.port = port;
        useDynamicAddress = true;
        loadProperties();
    }

    private void loadProperties() {
        ResourceBundle config = loadConfig();
        loadProperties(config);

        if (!useDynamicAddress)
            loadAddress(config);

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
        return ip + ":" + port;
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

    private void loadAddress(ResourceBundle config) {
        ip = config.getString(PROPERTY_LISTEN_ADDRESS);
        port = Integer.valueOf(config.getString(PROPERTY_LISTEN_PORT));
    }

    private void loadProperties(ResourceBundle config) {
        seeds = Arrays.asList(config.getString(PROPERTY_SEEDS).split(","));
        clusterName = config.getString(PROPERTY_CLUSTER_NAME);
        mode = config.getString(PROPERTY_MODE);
        Config.LOG_SERVER = config.getString(PROPERTY_LOG_SERVER);
        Config.LOG_MODE = config.getString(PROPERTY_LOG_MODE);
    }

    public void createTable() {
        terminal.initialize();
    }

    public void execute(String command) {
        terminal.execute(command.split("\\s+"));
    }

    public abstract void createTerminal();
    public abstract ResourceBundle loadConfig();
    public abstract Object getTable();
    public abstract void updateTable(Object o);
    public abstract String prepareAddNodeCommand();
    public abstract String prepareRemoveNodeCommand(String nodeIp, int nodePort);
}
