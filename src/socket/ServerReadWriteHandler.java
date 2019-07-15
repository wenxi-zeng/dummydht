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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Queue;

public class ServerReadWriteHandler implements Runnable, Attachable {
    private final SocketChannel socketChannel;
    private final Queue<Attachable> attachments;
    private SelectionKey selectionKey;

    private static final int READ_BUF_SIZE = 32 * 1024;
    private static final int WRITE_BUF_SIZE = 32 * 1024;
    private ByteBuffer[] _readBuf;
    private ByteBuffer[] _writeBuf;
    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private SocketServer.EventHandler eventHandler;
    private boolean processScheduled;
    private int size;

    public ServerReadWriteHandler(SocketChannel socketChannel, SocketServer.EventHandler eventHandler, Queue<Attachable> attchements) throws IOException {
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);
        this.eventHandler = eventHandler;
        this.attachments = attchements;
        this.processScheduled = false;

        this._writeBuf = new ByteBuffer[2];
        this._writeBuf[1] = ByteBuffer.allocate(WRITE_BUF_SIZE);
        this._writeBuf[0] = ByteBuffer.allocate(Integer.BYTES);
        this._readBuf = new ByteBuffer[2];
        this._readBuf[1] = ByteBuffer.allocate(READ_BUF_SIZE);
        this._readBuf[0] = ByteBuffer.allocate(Integer.BYTES);
        this.size = Integer.MIN_VALUE;
    }

    @Override
    public void run() {
        try {
            if (!this.selectionKey.isValid() && !this.socketChannel.isOpen()) return;
            if (this.selectionKey.isReadable()) {
                read();
            }
            else if (this.selectionKey.isWritable()) {
                write();
            }
        }
        catch (IOException ex) {
            attachments.add(new Recycler(selectionKey));
            SimpleLog.e(ex);
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
                _writeBuf[1] = ObjectConverter.getByteBuffer(response);
                _writeBuf[0].putInt(_writeBuf[1].remaining());
                _writeBuf[0].flip();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.selectionKey.interestOps(SelectionKey.OP_WRITE);
            this.selectionKey.selector().wakeup();
        }
    }

    private synchronized void read() throws IOException {
        if (this.socketChannel.read(_readBuf) == -1){
            this.selectionKey.cancel();
            this.socketChannel.close();
            return;
        }

        int numBytes;
        _readBuf[1].flip();

        if (size == Integer.MIN_VALUE) {
            _readBuf[0].flip();
            size = _readBuf[0].getInt();
        }
        numBytes = _readBuf[1].remaining();

         // SimpleLog.v("[" + socketChannel.getRemoteAddress() + "] Server: read bytes " + numBytes + ", total size: " + size);

        if (size < 0) {
            reset();
            return;
        }
        if (numBytes >= size) {
            // object fully arrived
            bos.write(ObjectConverter.getBytes(_readBuf[1], size));
            size = 0;
        }
        else {
            // object partially arrived, need to read again
            bos.write(ObjectConverter.getBytes(_readBuf[1], numBytes));
            size -= numBytes;
        }

        if (size <= 0 && size != Integer.MIN_VALUE) {
            if (!processScheduled) {
                processScheduled = true;
                SocketServer.getWorkerPool().execute(this::process);
            }
        }
        else {
            _readBuf[1].clear();
        }
    }

    private void write() throws IOException {
        this.socketChannel.write(_writeBuf);

        if (!_writeBuf[0].hasRemaining() && !_writeBuf[1].hasRemaining()) {
            reset();
            this.selectionKey.interestOps(SelectionKey.OP_READ);
            this.selectionKey.selector().wakeup();
        }
    }

    private void reset() {
        size = Integer.MIN_VALUE;
        processScheduled = false;
        _readBuf[0].clear();
        _readBuf[1].clear();
        _writeBuf[0].clear();
        _writeBuf[1].clear();
        bos.reset();
    }

    @Override
    public void attach(Selector selector) throws IOException {
        try {
            selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
            selectionKey.attach(this);
        }
        catch (CancelledKeyException ignored) {}
    }
}