package req;

import commonmodels.transport.Request;

public class RequestThread implements Runnable {

    private final RequestGenerator requestGenerator;

    private final RequestGenerateThreadCallBack callBack;

    public RequestThread(RequestGenerator requestGenerator, RequestGenerateThreadCallBack callBack) {
        this.requestGenerator = requestGenerator;
        this.callBack = callBack;
    }

    @Override
    public void run() {
        try {
            Request request = requestGenerator.next();
            callBack.onRequestGenerated(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public interface RequestGenerateThreadCallBack {
        void onRequestGenerated(Request request);
    }
}
