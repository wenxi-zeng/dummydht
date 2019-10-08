package filemanagement;

public class BucketMigrateInfoManager {

    private BucketMigrateInfoReporter reporter;

    private static volatile BucketMigrateInfoManager instance = null;

    private BucketMigrateInfoManager() {
        reporter = new BucketMigrateInfoReporter();
        reporter.start();
    }

    public static BucketMigrateInfoManager getInstance() {
        if (instance == null) {
            synchronized(BucketMigrateInfoManager.class) {
                if (instance == null) {
                    instance = new BucketMigrateInfoManager();
                }
            }
        }

        return instance;
    }

    public static void deleteInstance() {
        if (instance != null)
            instance.reporter.stop();
        instance = null;
    }

    public void record(BucketMigrateInfo migrateInfo, String token) {
        migrateInfo.setToken(token);
        reporter.report(migrateInfo);
    }
}
