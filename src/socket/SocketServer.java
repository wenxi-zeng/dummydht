package socket;

import com.sun.istack.internal.NotNull;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import util.SimpleLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class SocketServer implements Runnable{

    private final Selector selector;

    private final ServerSocketChannel serverSocketChannel;

    private final AtomicBoolean keepRunning = new AtomicBoolean(true);

    @NotNull
    private EventHandler eventHandler;

    private Queue<Attachable> attachments;

    private static volatile ExecutorService workerPool;

    private static final int WORKER_POOL_SIZE = 16;

    public SocketServer(int port, @NotNull EventHandler eventHandler) throws IOException {
        this.eventHandler = eventHandler;
        this.selector = Selector.open();
        this.attachments = new LinkedList<Attachable>() {
            @Override
            public boolean add(Attachable attachable) {
                boolean result = super.add(attachable);
                SimpleLog.v("Server: run, adding attachments");
                selector.wakeup();
                return result;
            }
        };
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        registerShutdownHook();
        attachments.add(new Acceptor(serverSocketChannel, attachments, eventHandler));
    }

    @Override
    public void run() {
        if (eventHandler != null)
            eventHandler.onBound();

        try {
            while (keepRunning.get()) {
                SimpleLog.v("Server: run, before select");
                registerAttachments();
                selector.select();
                SimpleLog.v("Server: run, after select");
                Iterator it = selector.selectedKeys().iterator();

                while (it.hasNext()) {
                    SimpleLog.v("Server: run, iterating");
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

    private void registerAttachments() throws IOException {
        if (attachments.isEmpty()) return;

        while (!attachments.isEmpty()) {
            Attachable attachable = attachments.poll();
            attachable.attach(selector);
        }
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
