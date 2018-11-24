package socket;

import util.ObjectConverter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;

public class SocketClient {

    public SocketClient() {
    }

    public void sendAsync(int port, Object data, ServerCallBack callBack) {
        sendAsync(new InetSocketAddress("localhost", port), data, callBack);
    }

    public void sendAsync(String address, int port, Object data, ServerCallBack callBack) {
        sendAsync(new InetSocketAddress(address, port), data, callBack);
    }

    private void sendAsync(InetSocketAddress inetSocketAddress, Object data, ServerCallBack callBack) {
        try (AsynchronousSocketChannel asynchronousSocketChannel = initAsynchronousSocketChannel()) {
            if (asynchronousSocketChannel != null) {
                asynchronousSocketChannel.connect(inetSocketAddress, null, new CompletionHandler<Void, Void>() {

                    @Override
                    public void completed(Void result, Void attachment) {
                        onServerConnected(asynchronousSocketChannel, data, callBack);
                    }

                    @Override
                    public void failed(Throwable exc, Void attachment) {
                        callBack.onFailure("Connection cannot be established!");
                    }
                });
            } else {
                callBack.onFailure("The asynchronous socket channel cannot be opened!");
            }

        } catch (IOException ex) {
            ex.printStackTrace();
            callBack.onFailure("An error occurred");
        }
    }

    public void send(int port, Object data, ServerCallBack callBack) {
        send(new InetSocketAddress("localhost", port), data, callBack);
    }

    public void send(String address, int port, Object data, ServerCallBack callBack) {
        send(new InetSocketAddress(address, port), data, callBack);
    }

    private void send(InetSocketAddress inetSocketAddress, Object data, ServerCallBack callBack) {
        try(AsynchronousSocketChannel asynchronousSocketChannel = initAsynchronousSocketChannel()) {
            if (asynchronousSocketChannel != null) {
                Void connect = asynchronousSocketChannel.connect(inetSocketAddress).get();

                if (connect == null)
                    onServerConnected(asynchronousSocketChannel, data, callBack);
                else
                    callBack.onFailure("Connection cannot be established!");
            }
            else {
                callBack.onFailure("The asynchronous socket channel cannot be opened!");
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            callBack.onFailure("An error occurred");
        }
    }

    private AsynchronousSocketChannel initAsynchronousSocketChannel() throws IOException {
        AsynchronousSocketChannel asynchronousSocketChannel = AsynchronousSocketChannel.open();

        if (asynchronousSocketChannel.isOpen()) {
            asynchronousSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
            asynchronousSocketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
            asynchronousSocketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        }

        return asynchronousSocketChannel;
    }

    private void onServerConnected(AsynchronousSocketChannel asynchronousSocketChannel, Object data, ServerCallBack callBack) {
        try {
            System.out.println("Successfully connected at: " + asynchronousSocketChannel.getRemoteAddress());

            final ByteBuffer sendBuffer = ObjectConverter.getByteBuffer(data);
            final ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
            // transmitting data
            asynchronousSocketChannel.write(sendBuffer).get();

            // read response
            asynchronousSocketChannel.read(buffer).get();
            buffer.flip();
            Object o = ObjectConverter.getObject(buffer);
            callBack.onResponse(o);
        } catch (IOException | InterruptedException | ExecutionException ex) {
            System.err.println(ex);
        } finally {
            try {
                asynchronousSocketChannel.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public interface ServerCallBack {
        void onResponse(Object o);
        void onFailure(String error);
    }
}
