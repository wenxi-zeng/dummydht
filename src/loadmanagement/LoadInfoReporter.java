package loadmanagement;

import commands.ProxyCommand;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import socket.SocketClient;
import util.Config;

import java.util.concurrent.*;

public class LoadInfoReporter implements SocketClient.ServerCallBack {

    private ScheduledExecutorService scheduledExecutorService;

    private ThreadPoolExecutor threadService;

    private SocketClient socketClient;

    private final LoadInfoManager loadInfoManager;

    private final long reportInterval;

    public LoadInfoReporter(LoadInfoManager loadInfoManager) {
        this.loadInfoManager = loadInfoManager;
        this.reportInterval = Config.getInstance().getLoadInfoReportInterval();
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(8);
        threadService = new ThreadPoolExecutor(1, 30, 1, TimeUnit.SECONDS, workQueue,
                new ThreadPoolExecutor.DiscardOldestPolicy());
        socketClient = new SocketClient();
    }

    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(
                () -> threadService.execute(this::report),
                reportInterval, reportInterval, TimeUnit.MILLISECONDS
        );
    }

    private void report() {
        Request request = new Request()
                .withHeader(ProxyCommand.UPDATELOAD.name())
                .withLargeAttachment(loadInfoManager.getLoadInfo());
        socketClient.send(
                Config.getInstance().getSeeds().get(0),
                request,
                this
        );
    }

    @Override
    public void onResponse(Response o) {

    }

    @Override
    public void onFailure(String error) {

    }
}
