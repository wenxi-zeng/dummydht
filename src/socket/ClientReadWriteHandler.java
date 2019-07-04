package socket;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
import statmanagement.StatInfoManager;
import util.ObjectConverter;
import util.SimpleLog;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class ClientReadWriteHandler implements Runnable {
    private final SocketChannel socketChannel;
    private final SelectionKey selectionKey;
    private final SocketClient.ServerCallBack callBack;
    private final Request data;

    private static final int READ_BUF_SIZE = 32 * 1024;
    private ByteBuffer _readBuf = ByteBuffer.allocate(READ_BUF_SIZE);
    private ByteBuffer _writeBuf;
    private ByteArrayOutputStream bos = new ByteArrayOutputStream();

    public ClientReadWriteHandler(Selector selector, SocketChannel socketChannel, Request data, SocketClient.ServerCallBack callBack) throws IOException {
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);
        this.data = data;
        this.callBack = callBack;

        this._writeBuf = ObjectConverter.getByteBuffer(data);
        this.selectionKey = this.socketChannel.register(selector, SelectionKey.OP_WRITE);
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

    private void process() {
        byte[] byteArray = bos.toByteArray();
        int respSize = byteArray.length;
        Object o = ObjectConverter.getObject(byteArray);
        SimpleLog.i(data);
        if (o instanceof Response) {
            Response resp = (Response) o;
            StatInfoManager.getInstance().statResponse(data, resp, respSize);
            callBack.onResponse(data, resp);
        }
        else {
            StatInfoManager.getInstance().statRoundTripFailure(data);
            callBack.onFailure(data, "NIO read failure");
        }
    }

    private synchronized void read() throws IOException {
        int numBytes = this.socketChannel.read(_readBuf);

        if (numBytes == -1) {
            socketChannel.close();
            selectionKey.cancel();

            process();

            _readBuf.clear();
            _writeBuf.clear();
            bos.reset();
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
            this.selectionKey.interestOps(SelectionKey.OP_READ);
        }
        else {
            this.selectionKey.interestOps(SelectionKey.OP_WRITE);
        }
    }
}