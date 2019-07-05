package socket;

import util.SimpleLog;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Queue;

public class Acceptor implements Runnable, Attachable {

    private final ServerSocketChannel serverSocketChannel;

    private SelectionKey selectionKey;

    private final Queue<Attachable> attachments;

    private final SocketServer.EventHandler eventHandler;

    public Acceptor(ServerSocketChannel serverSocketChannel, Queue<Attachable> attachments, SocketServer.EventHandler eventHandler) {
        this.serverSocketChannel = serverSocketChannel;
        this.attachments = attachments;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            SimpleLog.v("Server: run, before accept");
            SocketChannel socketChannel = serverSocketChannel.accept();
            SimpleLog.v("Server: run, after accept");
            if (socketChannel != null) {
                attachments.add(new ServerReadWriteHandler(socketChannel, eventHandler));
            }
        }
        catch (IOException ex) {
            selectionKey.cancel();
            ex.printStackTrace();
        }
    }

    @Override
    public void attach(Selector selector) throws IOException{
        selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        selectionKey.attach(this);
    }
}