package loadmanagement;

import filemanagement.LocalFileManager;
import util.Config;

public class LoadInfoManager {

    private LoadInfo loadInfo;

    private static volatile LoadInfoManager instance = null;

    private LoadInfoManager() {
        loadInfo = new LoadInfo();
        loadInfo.setReadFactor(Config.getInstance().getReadFactor());
    }

    public static LoadInfoManager getInstance() {
        if (instance == null) {
            synchronized(LoadInfoManager.class) {
                if (instance == null) {
                    instance = new LoadInfoManager();
                }
            }
        }

        return instance;
    }

    public LoadInfo getLoadInfo() {
        return LocalFileManager.getInstance().updateLoadInfo(loadInfo);
    }

    public void incrementNumberOfRead() {
        loadInfo.incrementNumberOfRead();
    }

    public void increaseWriteLoad(long write) {
        loadInfo.increaseWriteLoad(write);
    }

    public void incrementNumberOfHits() {
        loadInfo.incrementNumberOfHits();
    }

    public void incrementNumberOfMiss() {
        loadInfo.incrementNumberOfMiss();
    }
}
