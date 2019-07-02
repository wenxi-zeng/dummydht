package statmanagement;

import data.DummyDhtRepository;
import socket.UDPClient;
import util.Config;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StatInfoReporter {

    private final StatInfoManager statInfoManager;

    private final DummyDhtRepository repo;

    private ExecutorService executor;

    private UDPClient client;

    private boolean isEnableStatServer;

    public StatInfoReporter(StatInfoManager statInfoManager) {
        this.statInfoManager = statInfoManager;
        this.repo = DummyDhtRepository.getInstance();
        this.executor = Executors.newSingleThreadExecutor();
        this.isEnableStatServer = Config.getInstance().isEnableStatServer();
        //if (this.isEnableStatServer) {
            try {
                this.client = new UDPClient(Config.getInstance().getStatServer());
            } catch (SocketException e) {
                e.printStackTrace();
            }
        //}
    }

    public void report() {
        executor.execute(this::flush);
    }

    private void flush() {
        while (!statInfoManager.getQueue().isEmpty()) {
            StatInfo info = statInfoManager.getQueue().poll();
            try {
                if (isEnableStatServer) {
                    client.send(info);
                }
                repo.put(info);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
