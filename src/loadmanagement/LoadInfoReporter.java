package loadmanagement;

import commonmodels.LoadInfoReportHandler;
import data.DummyDhtRepository;
import util.Config;

import java.util.concurrent.*;

public class LoadInfoReporter {

    private ScheduledExecutorService scheduledExecutorService;

    private ThreadPoolExecutor threadService;

    private LoadInfoReportHandler handler;

    private final LoadInfoManager loadInfoManager;

    private final DummyDhtRepository repo;

    private final long reportInterval;

    public LoadInfoReporter(LoadInfoManager loadInfoManager) {
        this.loadInfoManager = loadInfoManager;
        this.reportInterval = Config.getInstance().getLoadInfoReportInterval();
        repo = DummyDhtRepository.getInstance();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(8);
        threadService = new ThreadPoolExecutor(1, 30, 1, TimeUnit.SECONDS, workQueue,
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(
                () -> threadService.execute(this::report),
                reportInterval, reportInterval, TimeUnit.MILLISECONDS
        );
    }

    public void stop () {
        DummyDhtRepository.deleteInstance();
        scheduledExecutorService.shutdownNow();
    }

    private void report() {
        LoadInfo loadInfo = loadInfoManager.getLoadInfo();
        loadInfo.setReportTime(System.currentTimeMillis());
        repo.put(loadInfo);
        if (handler != null)
            handler.onLoadInfoReported(loadInfo);
    }

    public void setHandler(LoadInfoReportHandler handler) {
        this.handler = handler;
    }
}
