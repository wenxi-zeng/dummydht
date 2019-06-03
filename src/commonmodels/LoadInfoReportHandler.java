package commonmodels;

import loadmanagement.LoadInfo;

public interface LoadInfoReportHandler {
    void onLoadInfoReported(LoadInfo loadInfo);
}
