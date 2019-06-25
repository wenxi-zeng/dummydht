package socket;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
import statmanagement.StatInfoManager;
import util.ObjectConverter;
import util.SimpleLog;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SocketClient {

    public SocketClient() {
    }

    public void sendAsync(int port, Request data, ServerCallBack callBack) {
        sendAsync(new InetSocketAddress("localhost", port), data, callBack);
    }

    public void sendAsync(String address, int port, Request data, ServerCallBack callBack) {
        sendAsync(new InetSocketAddress(address, port), data, callBack);
    }

    private void sendAsync(InetSocketAddress inetSocketAddress, Request data, ServerCallBack callBack) {
        try (AsynchronousSocketChannel asynchronousSocketChannel = initAsynchronousSocketChannel()) {
            if (asynchronousSocketChannel != null) {
                asynchronousSocketChannel.connect(inetSocketAddress, null, new CompletionHandler<Void, Void>() {

                    @Override
                    public void completed(Void result, Void attachment) {
                        onServerConnected(asynchronousSocketChannel, data, callBack);
                    }

                    @Override
                    public void failed(Throwable exc, Void attachment) {
                        StatInfoManager.getInstance().statRoundTripFailure(data);
                        callBack.onFailure(data, "Connection cannot be established!");
                    }
                });
            } else {
                StatInfoManager.getInstance().statRoundTripFailure(data);
                callBack.onFailure(data, "The asynchronous socket channel cannot be opened!");
            }

        } catch (IOException ex) {
            ex.printStackTrace();
            StatInfoManager.getInstance().statRoundTripFailure(data);
            callBack.onFailure(data, "An error occurred");
        }
    }

    public void send(int port, Request data, ServerCallBack callBack) {
        send(new InetSocketAddress("localhost", port), data, callBack);
    }

    public void send(String address, int port, Request data, ServerCallBack callBack) {
        send(new InetSocketAddress(address, port), data, callBack);
    }

    public void send(String address, Request data, ServerCallBack callBack) {
        String[] args = address.split(" ");
        String[] address1 = args[0].split(":");
        send(new InetSocketAddress(address1[0], Integer.valueOf(address1[1])), data, callBack);
    }

    public void send(InetSocketAddress inetSocketAddress, Request data, ServerCallBack callBack) {
        try(AsynchronousSocketChannel asynchronousSocketChannel = initAsynchronousSocketChannel()) {
            if (asynchronousSocketChannel != null) {
                Void connect = asynchronousSocketChannel.connect(inetSocketAddress).get(3, TimeUnit.SECONDS);

                if (connect == null)
                    onServerConnected(asynchronousSocketChannel, data, callBack);
                else
                    callBack.onFailure(data, "Connection cannot be established!");
            }
            else {
                callBack.onFailure(data, "The asynchronous socket channel cannot be opened!");
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            StatInfoManager.getInstance().statRoundTripFailure(data);
            callBack.onFailure(data, "Remote " + inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort() + " throws "  + e.getMessage());
        } catch (TimeoutException e) {
            StatInfoManager.getInstance().statRoundTripFailure(data);
            callBack.onFailure(data, "Remote " + inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort() + " time out");
        }
    }

    private AsynchronousSocketChannel initAsynchronousSocketChannel() throws IOException {
        AsynchronousSocketChannel asynchronousSocketChannel = AsynchronousSocketChannel.open();

        if (asynchronousSocketChannel.isOpen()) {
            asynchronousSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 32 * 1024);
            asynchronousSocketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 32 * 1024);
            asynchronousSocketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }

        return asynchronousSocketChannel;
    }

    private void onServerConnected(AsynchronousSocketChannel asynchronousSocketChannel, Request data, ServerCallBack callBack) {
        boolean success = false;
        Object o = null;
        int respSize = 0;
        String message = "Unknown server connection error";

        try {
            final ByteBuffer sendBuffer = ObjectConverter.getByteBuffer(data);
            final ByteBuffer buffer = ByteBuffer.allocateDirect(8 * 1024);
            // transmitting data
            Future<Integer> future = asynchronousSocketChannel.write(sendBuffer);
            while (future != null) {
                future.get();
                if (sendBuffer.remaining() > 0)
                    future = asynchronousSocketChannel.write(sendBuffer);
                else
                    break;
            }
            asynchronousSocketChannel.shutdownOutput();

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            // read response
            while(asynchronousSocketChannel.read(buffer).get() != -1) {
                buffer.flip();
                bos.write(ObjectConverter.getBytes(buffer));

                if (buffer.hasRemaining()) {
                    buffer.compact();
                } else {
                    buffer.clear();
                }
            }

            byte[] byteArray = bos.toByteArray();
            respSize = byteArray.length;
            o = ObjectConverter.getObject(byteArray);
            success = true;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            message = sw.toString();
        } finally {
            try {
                asynchronousSocketChannel.shutdownOutput();
                asynchronousSocketChannel.close();

                SimpleLog.i(data);
                if (success && o instanceof Response) {
                    Response resp = (Response) o;
                    StatInfoManager.getInstance().statResponse(data, resp, respSize);
                    callBack.onResponse(data, resp);
                }
                else {
                    StatInfoManager.getInstance().statRoundTripFailure(data);
                    callBack.onFailure(data, message);
                }

            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public interface ServerCallBack {
        void onResponse(Request request, Response response);
        void onFailure(Request request, String error);
    }
}
