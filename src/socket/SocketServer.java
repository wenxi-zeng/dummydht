package socket;

import com.sun.istack.internal.NotNull;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SocketServer implements Runnable{

    private final Selector selector;

    private final ServerSocketChannel serverSocketChannel;

    private final AtomicBoolean keepRunning = new AtomicBoolean(true);

    @NotNull
    private EventHandler eventHandler;

    private static volatile ExecutorService workerPool;

    private static final int WORKER_POOL_SIZE = 16;

    public SocketServer(int port, @NotNull EventHandler eventHandler) throws IOException {
        this.eventHandler = eventHandler;
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        registerShutdownHook();
        new Acceptor(selector, serverSocketChannel, eventHandler);
    }

    @Override
    public void run() {
        if (eventHandler != null)
            eventHandler.onBound();

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

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public static ExecutorService getWorkerPool() {
        if (workerPool == null) {
            synchronized(SocketServer.class) {
                if (workerPool == null) {
                    workerPool = Executors.newFixedThreadPool(WORKER_POOL_SIZE);
                }
            }
        }

        return workerPool;
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (serverSocketChannel != null && serverSocketChannel.isOpen()) {
                    selector.close();
                    serverSocketChannel.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public interface EventHandler {
        Response onReceived(Request o);
        void onBound();
    }
}
