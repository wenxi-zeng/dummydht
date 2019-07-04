package socket;

import commonmodels.transport.Request;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class Connector implements Runnable {

    private final Selector selector;

    private final Request data;

    private final SocketClient.ServerCallBack callBack;

    private final SocketChannel socketChannel;

    private final SelectionKey selectionKey;

    public Connector(Selector selector, SocketChannel socketChannel, Request data, SocketClient.ServerCallBack callBack) throws IOException{
        this.selector = selector;
        this.socketChannel = socketChannel;
        this.data = data;
        this.callBack = callBack;

        selectionKey = socketChannel.register(selector, SelectionKey.OP_CONNECT);
        selectionKey.attach(this);
    }

    @Override
    public void run() {
        try {
            socketChannel.finishConnect();
            new ClientReadWriteHandler(selector, socketChannel, data, callBack);
        }
        catch (IOException ex) {
            selectionKey.cancel();
            ex.printStackTrace();
        }
    }
}