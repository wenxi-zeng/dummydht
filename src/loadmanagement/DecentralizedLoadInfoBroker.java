package loadmanagement;

public class DecentralizedLoadInfoBroker extends LoadInfoBroker {

    private static volatile DecentralizedLoadInfoBroker instance = null;

    private DecentralizedLoadInfoBroker() {
    }

    public static DecentralizedLoadInfoBroker getInstance() {
        if (instance == null) {
            synchronized(DecentralizedLoadInfoBroker.class) {
                if (instance == null) {
                    instance = new DecentralizedLoadInfoBroker();
                }
            }
        }

        return instance;
    }
}
