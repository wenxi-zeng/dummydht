package socket;

import com.sun.istack.internal.NotNull;
import util.ObjectConverter;
import util.SimpleLog;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class UDPServer extends Thread {

    private DatagramSocket socket;
    private byte[] buf = new byte[65535];

    @NotNull
    private EventHandler eventHandler;

    public UDPServer(int port, EventHandler eventHandler) throws SocketException {
        this.socket = new DatagramSocket(port);
        this.eventHandler = eventHandler;
    }

    public void run() {
        while (true) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(packet);
                Object o = ObjectConverter.getObject(packet.getData());
                if (o != null) {
                    eventHandler.onReceived(o);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public interface EventHandler {
        void onReceived(Object o);
    }
}