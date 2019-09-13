package req;

import req.gen.RequestGenerator;

import java.util.concurrent.*;

public class RequestService {

    private final int numberOfThreads;

    private final RequestGenerator generator;

    private final long interArrivalTime;

    private final RequestThread.RequestGenerateThreadCallBack callBack;

    private final int numOfRequests;

    private final CountDownLatch latch;

    public RequestService(int numberOfThreads, long interArrivalTime, int numOfRequests, RequestGenerator generator, RequestThread.RequestGenerateThreadCallBack callBack) {
        this.numberOfThreads = numberOfThreads;
        this.generator = generator;
        this.interArrivalTime = interArrivalTime;
        this.numOfRequests = numOfRequests;
        this.callBack = callBack;
        this.latch = new CountDownLatch(numberOfThreads);
    }

    public void start() {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; ++i) {
            pool.scheduleAtFixedRate(new RequestThread(generator, latch, i, numOfRequests, callBack),
                    0, interArrivalTime, TimeUnit.MILLISECONDS);
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) {}
        finally {
            pool.shutdownNow();
        }
    }
}
