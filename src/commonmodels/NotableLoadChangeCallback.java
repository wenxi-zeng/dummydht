package commonmodels;

import commonmodels.transport.Request;

import java.util.List;

public interface NotableLoadChangeCallback {

    void onRequestAvailable(List<Request> request);

}
