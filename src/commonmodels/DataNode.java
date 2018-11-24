package commonmodels;

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

    public DataNode() {
        useDynamicAddress = false;
        initialize();
    }

    public DataNode(int port) {
        this.port = port;
        useDynamicAddress = true;
        initialize();
    }

    public void initialize() {
        ResourceBundle config = loadConfig();
        loadProperties(config);

        if (useDynamicAddress)
            initAddress();
        else
            loadAddress(config);

        initTerminal();
    }

    public void destroy() {

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
    }

    public abstract void initTerminal();
    public abstract ResourceBundle loadConfig();
}
