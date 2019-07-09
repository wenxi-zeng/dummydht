package socket;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
            SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                attachments.add(new ServerReadWriteHandler(socketChannel, eventHandler, attachments));
            }
        }
        catch (IOException ex) {
            attachments.add(new Recycler(selectionKey));
            ex.printStackTrace();
        }
    }

    @Override
    public void attach(Selector selector) throws IOException{
        selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        selectionKey.attach(this);
    }
}