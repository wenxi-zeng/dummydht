package socket;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
import statmanagement.StatInfoManager;
import util.ObjectConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class ServerReadWriteHandler implements Runnable {
    private final SocketChannel socketChannel;
    private final SelectionKey selectionKey;

    private static final int READ_BUF_SIZE = 32 * 1024;
    private static final int WRiTE_BUF_SIZE = 32 * 1024;
    private ByteBuffer _readBuf = ByteBuffer.allocate(READ_BUF_SIZE);
    private ByteBuffer _writeBuf = ByteBuffer.allocate(WRiTE_BUF_SIZE);
    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private SocketServer.EventHandler eventHandler;

    public ServerReadWriteHandler(Selector selector, SocketChannel socketChannel, SocketServer.EventHandler eventHandler) throws IOException {
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);
        this.eventHandler = eventHandler;
        this.selectionKey = this.socketChannel.register(selector, SelectionKey.OP_READ);
        this.selectionKey.attach(this);
        selector.wakeup(); // let blocking select() return
    }

    @Override
    public void run() {
        try {
            if (this.selectionKey.isReadable()) {
                read();
            }
            else if (this.selectionKey.isWritable()) {
                write();
            }

            this.selectionKey.selector().wakeup();
        }
        catch (IOException ex) {
            selectionKey.cancel();
            ex.printStackTrace();
        }
    }

    private synchronized void process() {
        byte[] byteArray = bos.toByteArray();
        Object o = ObjectConverter.getObject(byteArray);
        if (o instanceof Request) {
            try {
                Request req = (Request) o;

                long stamp = StatInfoManager.getInstance().getStamp();
                StatInfoManager.getInstance().statRequest(req, stamp, byteArray.length);
                InetSocketAddress inetSocketAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
                req.setSender(inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort());
                Response response = eventHandler.onReceived(req);
                StatInfoManager.getInstance().statExecution(req, stamp);
                _writeBuf = ObjectConverter.getByteBuffer(response);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.selectionKey.interestOps(SelectionKey.OP_WRITE);
        }

    }

    private synchronized void read() throws IOException {
        int numBytes = this.socketChannel.read(_readBuf);

        if (numBytes == -1) {
            SocketServer.getWorkerPool().execute(this::process);
        }
        else {
            _readBuf.flip();
            bos.write(ObjectConverter.getBytes(_readBuf));
            if (_readBuf.hasRemaining()) {
                _readBuf.compact();
            } else {
                _readBuf.clear();
            }

            this.selectionKey.interestOps(SelectionKey.OP_READ);
        }
    }

    private void write() throws IOException {
        this.socketChannel.write(_writeBuf);
        if (_writeBuf.remaining() == 0) {
            _readBuf.clear();
            _writeBuf.clear();
            bos.reset();

            // Set the key's interest-set back to READ operation
            this.selectionKey.interestOps(SelectionKey.OP_READ);
        }
        else {
            this.selectionKey.interestOps(SelectionKey.OP_WRITE);
        }
    }
}