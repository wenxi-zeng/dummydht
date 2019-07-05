package socket;

import util.ObjectConverter;

import java.io.IOException;
import java.net.*;

public class UDPClient {
    private DatagramSocket socket;
    private SocketAddress address;

    public UDPClient(String address) throws SocketException {
        String[] address1 = address.split(":");
        socket = new DatagramSocket();
        this.address = new InetSocketAddress(address1[0], Integer.valueOf(address1[1]));
        socket.setReuseAddress(true);
    }

    public void send(Object msg) throws IOException {
        byte[] buf = ObjectConverter.getBytes(msg);
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address);
        socket.send(packet);
    }

    public void close() {
        socket.close();
    }
}