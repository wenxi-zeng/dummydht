package socket;

import commonmodels.transport.Request;
import util.SimpleLog;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Queue;

public class Connector implements Runnable, Attachable {

    private final Request data;

    private final SocketClient.ServerCallBack callBack;

    private final SocketChannel socketChannel;

    private final Queue<Attachable> attachments;

    private SelectionKey selectionKey;

    public Connector(SocketChannel socketChannel, Request data, Queue<Attachable> attachments, SocketClient.ServerCallBack callBack) {
        this.socketChannel = socketChannel;
        this.data = data;
        this.attachments = attachments;
        this.callBack = callBack;
    }

    @Override
    public void run() {
        try {
            SimpleLog.v("Client: run, before finishConnect");
            socketChannel.finishConnect();
            SimpleLog.v("Client: run, after finishConnect");
            attachments.add(new ClientReadWriteHandler(socketChannel, data, callBack, attachments));
        }
        catch (IOException ex) {
            attachments.add(new Recycler(selectionKey));
            ex.printStackTrace();
        }
    }

    @Override
    public void attach(Selector selector) throws IOException {
        selectionKey = socketChannel.register(selector, SelectionKey.OP_CONNECT);
        selectionKey.attach(this);
    }
}