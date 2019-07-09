package socket;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
import statmanagement.StatInfoManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class SocketClient implements Runnable{

    private Selector selector;

    private static volatile SocketClient instance = null;

    private final AtomicBoolean keepRunning = new AtomicBoolean(true);

    private final Queue<Attachable> attachments = new LinkedList<Attachable>() {
        @Override
        public boolean add(Attachable attachable) {
            boolean result = super.add(attachable);
            selector.wakeup();
            return result;
        }
    };

    private SocketClient() {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static SocketClient getInstance() {
        if (instance == null) {
            synchronized(SocketClient.class) {
                if (instance == null) {
                    instance = new SocketClient();
                    new Thread(instance).start();
                }
            }
        }

        return instance;
    }

    public static SocketClient newInstance() {
        SocketClient client = new SocketClient();
        new Thread(client).start();
        return client;
    }

    public void send(int port, Request data, ServerCallBack callBack) {
        send(new InetSocketAddress("localhost", port), data, callBack);
    }

    public void send(String address, int port, Request data, ServerCallBack callBack) {
        send(new InetSocketAddress(address, port), data, callBack);
    }

    public void send(String address, Request data, ServerCallBack callBack) {
        String[] args = address.split(" ");
        String[] address1 = args[0].split(":");
        send(new InetSocketAddress(address1[0], Integer.valueOf(address1[1])), data, callBack);
    }

    public void send(InetSocketAddress inetSocketAddress, Request data, ServerCallBack callBack) {
        try {
            registerRequest(inetSocketAddress, data, callBack);
        } catch (IOException e) {
            StatInfoManager.getInstance().statRoundTripFailure(data);
            callBack.onFailure(data, "Remote " + inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort() + " throws "  + e.getMessage());
        }
    }

    private void registerRequest(InetSocketAddress remote, Request data, ServerCallBack callBack) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(remote);
        attachments.add(new Connector(socketChannel, data, attachments, callBack));
    }

    @Override
    public void run() {
        try {
            while (keepRunning.get()) {
                registerAttachments();
                selector.select();
                Iterator it = selector.selectedKeys().iterator();

                while (it.hasNext()) {
                    SelectionKey sk = (SelectionKey) it.next();
                    it.remove();
                    Runnable r = (Runnable) sk.attachment(); // handler or acceptor callback/runnable
                    if (r != null) {
                        r.run();
                    }
                }
            }
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void registerAttachments() throws IOException {
        if (attachments.isEmpty()) return;

        while (!attachments.isEmpty()) {
            Attachable attachable = attachments.poll();
            if (attachable != null)
                attachable.attach(selector);
        }
    }

    public interface ServerCallBack {
        void onResponse(Request request, Response response);
        void onFailure(Request request, String error);
    }
}
