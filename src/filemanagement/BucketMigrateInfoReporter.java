package filemanagement;

import data.DummyDhtRepository;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class BucketMigrateInfoReporter {

    private final DummyDhtRepository repo;

    private ExecutorService executor;

    private BlockingQueue<BucketMigrateInfo> queue;

    private AtomicBoolean reporting;

    public BucketMigrateInfoReporter() {
        this.repo = DummyDhtRepository.getInstance();
        this.executor = Executors.newFixedThreadPool(2);
        this.queue = new LinkedBlockingQueue<>();
        this.reporting = new AtomicBoolean(true);
    }

    public void start() {
        executor.execute(this::consume);
    }

    public void stop () {
        DummyDhtRepository.deleteInstance();
        executor.shutdownNow();
    }

    public void report(BucketMigrateInfo bucketMigrateInfo) {
        executor.execute(() -> produce(bucketMigrateInfo));
    }

    private void produce(BucketMigrateInfo bucketMigrateInfo) {
        try {
            queue.put(bucketMigrateInfo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void consume() {
        try {
            while (reporting.get()) {
                BucketMigrateInfo bucketMigrateInfo = queue.take();
                repo.put(bucketMigrateInfo);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
