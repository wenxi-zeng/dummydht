package socket;

import com.sun.istack.internal.NotNull;
import util.ObjectConverter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketServer {

    private int port;

    private final Object lock = new Object();

    @NotNull
    private EventHandler eventHandler;

    public SocketServer(int port, @NotNull EventHandler eventHandler) {
        this.port = port;
        this.eventHandler = eventHandler;
    }

    public void start() throws Exception {
        ExecutorService connectPool = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withCachedThreadPool(connectPool, 1);

        try (AsynchronousServerSocketChannel channel = AsynchronousServerSocketChannel.open(group)) {
            registerShutdownHook(channel);

            if (channel.isOpen()) {
                channel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
                channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                channel.bind(new InetSocketAddress(port));
                channel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 1024);

                    @Override
                    public void completed(AsynchronousSocketChannel result, Void attachment) {

                        channel.accept(null, this);

                        if ((result != null) && (result.isOpen())) {
                            try {
                                while (result.read(buffer).get() != -1) {
                                    buffer.flip();
                                    Object o = ObjectConverter.getObject(buffer);
                                    if (o != null) {
                                        eventHandler.onReceived(result, o.toString().trim());
                                    }

                                    if (buffer.hasRemaining()) {
                                        buffer.compact();
                                    } else {
                                        buffer.clear();
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                try {
                                    result.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }

                    @Override
                    public void failed(Throwable exc, Void attachment) {
                        channel.accept(null, this);
                        throw new UnsupportedOperationException("Cannot accept connections!");
                    }
                });

                await();
            } else {
                throw new Exception("The asynchronous server-socket channel cannot be opened!");
            }
        }
    }

    private void registerShutdownHook(AsynchronousServerSocketChannel channel) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (channel != null && channel.isOpen())
                    channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private void await() throws InterruptedException {
        synchronized (lock) {
            lock.wait();
        }
    }

    public interface EventHandler {
        void onReceived(AsynchronousSocketChannel out, String message) throws Exception;
    }
}
