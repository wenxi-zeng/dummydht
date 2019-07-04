package socket;

import java.io.IOException;
import java.nio.channels.*;

public class Acceptor implements Runnable {

    private final Selector selector;

    private final ServerSocketChannel serverSocketChannel;

    private final SelectionKey selectionKey;

    private final SocketServer.EventHandler eventHandler;

    public Acceptor(Selector selector, ServerSocketChannel serverSocketChannel, SocketServer.EventHandler eventHandler) throws IOException {
        this.selector = selector;
        this.serverSocketChannel = serverSocketChannel;
        this.eventHandler = eventHandler;
        selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        selectionKey.attach(this);
    }

    @Override
    public void run() {
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                new ServerReadWriteHandler(selector, socketChannel, eventHandler);
            }
        }
        catch (IOException ex) {
            selectionKey.cancel();
            ex.printStackTrace();
        }
    }
}