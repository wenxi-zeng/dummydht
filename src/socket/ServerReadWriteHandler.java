package socket;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
import statmanagement.StatInfoManager;
import util.ObjectConverter;
import util.SimpleLog;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class ServerReadWriteHandler implements Runnable, Attachable {
    private final SocketChannel socketChannel;
    private SelectionKey selectionKey;

    private static final int READ_BUF_SIZE = 32 * 1024;
    private static final int WRiTE_BUF_SIZE = 32 * 1024;
    private ByteBuffer _readBuf = ByteBuffer.allocate(READ_BUF_SIZE);
    private ByteBuffer _writeBuf = ByteBuffer.allocate(WRiTE_BUF_SIZE);
    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private SocketServer.EventHandler eventHandler;

    public ServerReadWriteHandler(SocketChannel socketChannel, SocketServer.EventHandler eventHandler) throws IOException {
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            if (this.selectionKey.isReadable()) {
                SimpleLog.v("Server: run, before read");
                read();
                SimpleLog.v("Server: run, after read");
            }
            else if (this.selectionKey.isWritable()) {
                SimpleLog.v("Server: run, before write");
                write();
                SimpleLog.v("Server: run, after write");
            }
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

        SimpleLog.v("Server: run, reading " + numBytes);
        if (numBytes == -1) {
            socketChannel.close();
            selectionKey.cancel();
        }
        else {
            boolean readyFully = _readBuf.hasRemaining();
            _readBuf.flip();
            bos.write(ObjectConverter.getBytes(_readBuf));
            if (_readBuf.hasRemaining()) {
                _readBuf.compact();
            } else {
                _readBuf.clear();
            }

            if (readyFully) {
                SocketServer.getWorkerPool().execute(this::process);
            }
        }
    }

    private void write() throws IOException {
        this.socketChannel.write(_writeBuf);
        if (_writeBuf.remaining() == 0) {
            _readBuf.clear();
            _writeBuf.clear();
            bos.reset();

            this.selectionKey.interestOps(SelectionKey.OP_READ);
        }
    }

    @Override
    public void attach(Selector selector) throws IOException {
        selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
        selectionKey.attach(this);
    }
}