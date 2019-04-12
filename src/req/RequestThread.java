package req;

import commonmodels.transport.Request;

public class RequestThread implements Runnable {

    private final RequestGenerator requestGenerator;

    private final RequestGenerateThreadCallBack callBack;

    private final long interArrivalTime;

    public RequestThread(RequestGenerator requestGenerator, long interArrivalTime, RequestGenerateThreadCallBack callBack) {
        this.requestGenerator = requestGenerator;
        this.interArrivalTime = interArrivalTime;
        this.callBack = callBack;
    }

    @Override
    public void run() {
        while(true) {
            Request request;
            try {
                request = requestGenerator.next();
                callBack.onRequestGenerated(request);
                Thread.sleep(interArrivalTime);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public interface RequestGenerateThreadCallBack {
        void onRequestGenerated(Request request);
    }
}
