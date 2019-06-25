package socket;

import com.sun.istack.internal.NotNull;
import commonmodels.transport.Request;
import statmanagement.StatInfoManager;
import util.ObjectConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SocketServer {

    private int port;

    private final Object lock = new Object();

    @NotNull
    private EventHandler eventHandler;

    private EventResponsor eventResponsor = (out, response) -> {
        ByteBuffer buffer = ObjectConverter.getByteBuffer(response);
        Future<Integer> future = out.write(buffer);
        while (future != null) {
            future.get();
            if (buffer.remaining() > 0)
                future = out.write(buffer);
            else
                break;
        }
    };

    public SocketServer(int port, @NotNull EventHandler eventHandler) {
        this.port = port;
        this.eventHandler = eventHandler;
    }

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void start() throws Exception {
        ExecutorService connectPool = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withCachedThreadPool(connectPool, 1);

        try (AsynchronousServerSocketChannel channel = AsynchronousServerSocketChannel.open(group)) {
            registerShutdownHook(channel);

            if (channel.isOpen()) {
                channel.setOption(StandardSocketOptions.SO_RCVBUF, 32 * 1024);
                channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                channel.bind(new InetSocketAddress(port));
                channel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {

                    @Override
                    public void completed(AsynchronousSocketChannel result, Void attachment) {

                        channel.accept(null, this);

                        if ((result != null) && (result.isOpen())) {
                            try {
                                final ByteBuffer buffer = ByteBuffer.allocateDirect(8 * 1024);
                                ByteArrayOutputStream bos = new ByteArrayOutputStream();

                                while (result.read(buffer).get() != -1) {
                                    buffer.flip();
                                    bos.write(ObjectConverter.getBytes(buffer));

                                    if (buffer.hasRemaining()) {
                                        buffer.compact();
                                    } else {
                                        buffer.clear();
                                    }
                                }

                                byte[] byteArray = bos.toByteArray();
                                Object o = ObjectConverter.getObject(byteArray);
                                if (o instanceof Request) {
                                    Request req = (Request) o;

                                    long stamp = StatInfoManager.getInstance().getStamp();
                                    StatInfoManager.getInstance().statRequest(req, stamp, byteArray.length);
                                    eventHandler.onReceived(result, req, eventResponsor);
                                    StatInfoManager.getInstance().statExecution(req, stamp);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                try {
                                    result.shutdownOutput();
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

                if (eventHandler != null)
                    eventHandler.onBound();

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
        void onReceived(AsynchronousSocketChannel out, Request o, EventResponsor responsor) throws Exception;
        void onBound();
    }

    public interface EventResponsor {
        void reply (AsynchronousSocketChannel out, Object response) throws IOException, ExecutionException, InterruptedException;
    }
}
