package req;

import commonmodels.transport.Request;
import req.gen.RequestGenerator;
import req.rand.ExpGenerator;
import req.rand.RandomGenerator;
import req.rand.UniformGenerator;
import socket.SocketClient;

import java.util.concurrent.CountDownLatch;

public class RequestThread implements Runnable {

    private final RequestGenerator requestGenerator;

    private final RequestGenerateThreadCallBack callBack;

    private final SocketClient socketClient;

    private final CountDownLatch latch;

    private final int threadId;

    private final RandomGenerator possionGenerator;

    private int numOfRequests;

    public RequestThread(RequestGenerator requestGenerator, CountDownLatch latch, int threadId, int numOfRequests, double interArrivalRate, RequestGenerateThreadCallBack callBack) {
        this.requestGenerator = requestGenerator;
        this.callBack = callBack;
        this.latch = latch;
        this.threadId = threadId;
        this.numOfRequests = numOfRequests;
        this.possionGenerator = new ExpGenerator(interArrivalRate, 1, new UniformGenerator());
        socketClient = SocketClient.newInstance();
    }

    @Override
    public void run() {
        while (true) {
            if (numOfRequests == -1) {
                generate();
            } else if (numOfRequests > 0) {
                generate();
                numOfRequests--;
            } else {
                socketClient.stop();
                latch.countDown();
                Thread.currentThread().interrupt();
                break;
            }

            try {
                long delay = (long)possionGenerator.nextDouble();
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {}
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
