package statmanagement;

import data.DummyDhtRepository;
import socket.UDPClient;
import util.Config;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatInfoReporter {

    private final DummyDhtRepository repo;

    private ExecutorService executor;

    private UDPClient client;

    private boolean isEnableStatServer;

    private BlockingQueue<StatInfo> queue;

    private AtomicBoolean reporting;

    public StatInfoReporter() {
        this.repo = DummyDhtRepository.getInstance();
        this.executor = Executors.newFixedThreadPool(2);
        this.isEnableStatServer = Config.getInstance().isEnableStatServer();
        this.queue = new LinkedBlockingQueue<>();
        this.reporting = new AtomicBoolean(true);
        //if (this.isEnableStatServer) {
            try {
                this.client = new UDPClient(Config.getInstance().getStatServer());
            } catch (IOException e) {
                e.printStackTrace();
            }
        //}
    }

    public void start() {
        executor.execute(this::consume);
    }

    public void report(StatInfo statInfo) {
        executor.execute(() -> produce(statInfo));
    }

    private void produce(StatInfo statInfo) {
        try {
            queue.put(statInfo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void consume() {
        try {
            while (reporting.get()) {
                StatInfo statInfo = queue.take();
                if (isEnableStatServer) {
                    client.send(statInfo);
                    repo.put(statInfo);
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
