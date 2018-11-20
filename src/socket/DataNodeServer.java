package socket;

import commonmodels.DataNode;

import java.io.IOException;
import java.lang.ref.WeakReference;
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

public class DataNodeServer {

    private AsynchronousServerSocketChannel channel;

    private DataNode dataNode;

    public DataNodeServer(DataNode dataNode) {
        this.dataNode = dataNode;
    }

    public void start() throws Exception {
        ExecutorService connectPool = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withCachedThreadPool(connectPool, 1);
        channel = AsynchronousServerSocketChannel.open(group);

        if (channel.isOpen()) {
            channel.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024);
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(dataNode.getIp(), dataNode.getPort()));
            channel.accept(null, new ClientCallBack(this));

            System.in.read();
        } else {
            throw new Exception("The asynchronous server-socket channel cannot be opened!");
        }
    }

    public void processMessage(AsynchronousSocketChannel client, String message) throws IOException {
        String cmdLine[] = message.split("\\s+");
        dataNode.getTerminal().execute(cmdLine);
        client.close();
    }

    class ClientCallBack implements CompletionHandler<AsynchronousSocketChannel, Void> {

        private final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        private final WeakReference<DataNodeServer> ref;

        ClientCallBack(DataNodeServer ref) {
            this.ref = new WeakReference<>(ref);
        }

        @Override
        public void completed(AsynchronousSocketChannel result, Void attachment) {
            DataNodeServer server = ref.get();
            if (server == null) {
                return;
            }

            AsynchronousServerSocketChannel asynchronousServerSocketChannel = server.channel;
            asynchronousServerSocketChannel.accept(null, this);

            if ((result != null) && (result.isOpen())) {
                try {
                    StringBuilder message = new StringBuilder();

                    while (result.read(buffer).get() != -1) {
                        buffer.flip();

                        String temp = new String(buffer.array());
                        message.append(temp);

                        if (buffer.hasRemaining()) {
                            buffer.compact();
                        } else {
                            buffer.clear();
                        }
                    }

                    server.processMessage(result, message.toString().trim());
                }
                catch (InterruptedException | ExecutionException | IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            DataNodeServer server = ref.get();

            if (server == null) {
                throw new UnsupportedOperationException("Error. Data Node server is terminated!");
            }

            AsynchronousServerSocketChannel asynchronousServerSocketChannel = server.channel;
            asynchronousServerSocketChannel.accept(null, this);
            throw new UnsupportedOperationException("Cannot accept connections!");
        }
    }
}
