package entries;

import ceph.CephDataNode;
import commonmodels.DataNode;
import elastic.ElasticDataNode;
import ring.RingDataNode;
import socket.SocketServer;
import util.SimpleLog;
import util.URIHelper;

import java.net.URI;
import java.nio.channels.AsynchronousSocketChannel;

public class Proxy implements SocketServer.EventHandler  {

    private SocketServer socketServer;

    private DataNode dataNode;

    public static void main(String[] args){
        if (args.length > 1)
        {
            System.err.println ("Usage: Proxy [DHT type]");
            return;
        }

        String type = "ring";
        if (args.length > 0)
        {
            type = args[0];
        }

        try {
            Proxy proxy = new Proxy(type);
            SimpleLog.with(proxy.dataNode.getIp(), proxy.dataNode.getPort());
            SimpleLog.i("Proxy: started");
            proxy.exec();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Proxy(String type) throws Exception {
        if (type.equalsIgnoreCase("ring"))
            dataNode = new RingDataNode();
        else if (type.equalsIgnoreCase("elastic"))
            dataNode = new ElasticDataNode();
        else if (type.equalsIgnoreCase("ceph"))
            dataNode = new CephDataNode();
        else
            throw new Exception("Invalid DHT type");

        if (dataNode.getSeeds() == null || dataNode.getSeeds().size() < 1)
            throw new Exception("Proxy not specified");

        URI uri = URIHelper.getGossipURI(dataNode.getSeeds().get(0));
        dataNode.setPort(uri.getPort());
        dataNode.setIp(uri.getHost());
        dataNode.setUseDynamicAddress(true);
        this.socketServer = new SocketServer(dataNode.getPort(), this);
    }

    private void exec() throws Exception {
        socketServer.start();
    }

    @Override
    public void onReceived(AsynchronousSocketChannel out, String message) throws Exception {

    }
}
