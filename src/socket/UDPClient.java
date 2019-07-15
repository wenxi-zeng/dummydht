package socket;

import util.ObjectConverter;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class UDPClient {

    private DatagramChannel channel;

    public UDPClient(String address) throws IOException {
        String[] address1 = address.split(":");
        InetSocketAddress socketAddress = new InetSocketAddress(address1[0], Integer.valueOf(address1[1]));
        DatagramChannel datagramChannel = DatagramChannel.open();
        datagramChannel.socket().setReuseAddress(true);
        channel = datagramChannel.connect(socketAddress);
    }

    public void send(Object msg) throws IOException {
        ByteBuffer _writeBuf = ObjectConverter.getByteBuffer(msg);
        channel.write(_writeBuf);
    }
}