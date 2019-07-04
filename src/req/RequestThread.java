package req;

import commonmodels.transport.Request;
import socket.SocketClient;

public class RequestThread implements Runnable {

    private final RequestGenerator requestGenerator;

    private final RequestGenerateThreadCallBack callBack;

    private final SocketClient socketClient;

    public RequestThread(RequestGenerator requestGenerator, RequestGenerateThreadCallBack callBack) {
        this.requestGenerator = requestGenerator;
        this.callBack = callBack;
        socketClient = SocketClient.newInstance();
    }

    @Override
    public void run() {
        try {
            Request request = requestGenerator.next();
            callBack.onRequestGenerated(request, socketClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public interface RequestGenerateThreadCallBack {
        void onRequestGenerated(Request request, SocketClient client);
    }
}
