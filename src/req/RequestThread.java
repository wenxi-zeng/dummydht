package req;

import commonmodels.transport.Request;
import req.gen.RequestGenerator;
import socket.SocketClient;

import java.util.concurrent.CountDownLatch;

public class RequestThread implements Runnable {

    private final RequestGenerator requestGenerator;

    private final RequestGenerateThreadCallBack callBack;

    private final SocketClient socketClient;

    private final CountDownLatch latch;

    private final int threadId;

    private int numOfRequests;

    public RequestThread(RequestGenerator requestGenerator, CountDownLatch latch, int threadId, int numOfRequests, RequestGenerateThreadCallBack callBack) {
        this.requestGenerator = requestGenerator;
        this.callBack = callBack;
        this.latch = latch;
        this.threadId = threadId;
        this.numOfRequests = numOfRequests;
        socketClient = SocketClient.newInstance();
    }

    @Override
    public void run() {
        if (numOfRequests == -1) {
            generate();
        }
        else if (numOfRequests > 0) {
            generate();
            numOfRequests--;
        }
        else {
            socketClient.stop();
            latch.countDown();
            Thread.currentThread().interrupt();
        }
    }

    private void generate() {
        try {
            Request request = requestGenerator.nextFor(threadId);
            callBack.onRequestGenerated(request, socketClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public interface RequestGenerateThreadCallBack {
        void onRequestGenerated(Request request, SocketClient client);
    }
}
