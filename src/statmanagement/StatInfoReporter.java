package statmanagement;

import socket.UDPClient;
import util.Config;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StatInfoReporter {

    private final StatInfoManager statInfoManager;

    private ExecutorService executor;

    private UDPClient client;

    public StatInfoReporter(StatInfoManager statInfoManager) {
        this.statInfoManager = statInfoManager;
        this.executor = Executors.newFixedThreadPool(2);
        try {
            this.client = new UDPClient(Config.getInstance().getStatServer());
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public void report() {
        executor.execute(this::flush);
    }

    private void flush() {
        while (!statInfoManager.getQueue().isEmpty()) {
            StatInfo info = statInfoManager.getQueue().poll();
            try {
                client.send(info);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}