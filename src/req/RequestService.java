package req;

import java.util.concurrent.*;

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

    public void start() {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; ++i) {
            pool.scheduleAtFixedRate(new RequestThread(generator, callBack),
                    0, interArrivalTime, TimeUnit.MILLISECONDS);
        }
    }
}
