package loadmanagement;

import commonmodels.LoadInfoReportHandler;
import data.DummyDhtRepository;
import util.Config;

import java.util.concurrent.*;

public class LoadInfoReporter {

    private ScheduledExecutorService scheduledExecutorService;

    private LoadInfoReportHandler handler;

    private final LoadInfoManager loadInfoManager;

    private final DummyDhtRepository repo;

    private final long reportInterval;

    private int counter;

    public LoadInfoReporter(LoadInfoManager loadInfoManager) {
        this.loadInfoManager = loadInfoManager;
        this.reportInterval = Config.getInstance().getLoadInfoReportInterval();
        repo = DummyDhtRepository.getInstance();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        counter = 0;
        scheduledExecutorService.scheduleAtFixedRate(
                this::report,
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
        loadInfo.setSerialNumber(++counter);
        repo.put(loadInfo);
        if (handler != null)
            handler.onLoadInfoReported(loadInfo);
    }

    public void setHandler(LoadInfoReportHandler handler) {
        this.handler = handler;
    }
}
