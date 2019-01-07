package req;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RequestService {

    private final int numberOfThreads;

    private final RequestGenerator generator;

    private final long interArrivalTime;

    private final RequestThread.RequestGenerateThreadCallBack callBack;

    public RequestService(int numberOfThreads, long interArrivalTime, RequestGenerator generator, RequestThread.RequestGenerateThreadCallBack callBack) {
        this.numberOfThreads = numberOfThreads;
        this.generator = generator;
        this.interArrivalTime = interArrivalTime;
        this.callBack = callBack;
    }

    public void start() throws InterruptedException {
        CountDownLatch start = new CountDownLatch(numberOfThreads);
        ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; ++i) {
            pool.execute(new RequestThread(generator,
                    interArrivalTime,
                    callBack));
        }

        start.await(5, TimeUnit.SECONDS);
        pool.shutdownNow();
        pool.awaitTermination(1, TimeUnit.SECONDS);
    }
}
