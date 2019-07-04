package socket;

import util.ObjectConverter;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class UDPClient implements Runnable{

    private SocketAddress address;

    private Selector selector;

    private final AtomicBoolean keepRunning = new AtomicBoolean(true);

    private final DatagramChannel socketChannel;

    public UDPClient(String address) throws IOException {
        selector = Selector.open();
        String[] address1 = address.split(":");
        this.address = new InetSocketAddress(address1[0], Integer.valueOf(address1[1]));

        socketChannel = DatagramChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.bind(null);
    }

    public void send(Object msg) throws IOException {
        registerRequest(msg);
    }

    private void registerRequest(Object msg) throws IOException {
        new Handler(address, selector, socketChannel, msg);
    }

    @Override
    public void run() {
        try {
            while (keepRunning.get()) {
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

    private class Handler implements Runnable{

        private final SocketAddress _address;

        private final DatagramChannel _datagramChannel;

        private final SelectionKey _selectionKey;

        private final ByteBuffer _writeBuf;

        private Handler(SocketAddress address, Selector selector, DatagramChannel datagramChannel, Object msg) throws IOException {
            _address = address;
            _datagramChannel = datagramChannel;
            _writeBuf = ObjectConverter.getByteBuffer(msg);
            _selectionKey = datagramChannel.register(selector, SelectionKey.OP_WRITE);
            _selectionKey.attach(this);
            selector.wakeup();
        }

        @Override
        public void run() {
            try {
                write();

                _selectionKey.interestOps(SelectionKey.OP_WRITE);
                _selectionKey.selector().wakeup();
            } catch (IOException e) {
                _selectionKey.cancel();
                e.printStackTrace();
            }
        }

        private void write() throws IOException {
            _datagramChannel.send(_writeBuf, _address);
            _datagramChannel.close();
            _selectionKey.cancel();
            _writeBuf.clear();
        }
    }
}
