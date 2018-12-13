package commonmodels;

import util.Config;
import util.SimpleLog;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
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

        if (useDynamicAddress)
            initAddress();
        else
            loadAddress(config);

        SimpleLog.with(ip, port);
        createTerminal();
    }

    public void destroy() {
        if (terminal != null)
            terminal.destroy();
    }

    private void initAddress() {
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        } catch (UnknownHostException | SocketException e) {
            e.printStackTrace();
        }
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

    public void setUseDynamicAddress(boolean useDynamicAddress) {
        this.useDynamicAddress = useDynamicAddress;
    }

    public void setPort(int port) {
        this.port = port;
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

    public abstract void createTerminal();
    public abstract ResourceBundle loadConfig();
    public abstract void onNodeUp(String cluster, String ip, int port);
    public abstract void onNodeDown(String ip, int port);
    public abstract Object getTable();
    public abstract void updateTable(Object o);
}
