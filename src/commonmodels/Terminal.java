package commonmodels;

import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

public interface Terminal {
    void initialize();
    void destroy();
    void printInfo();
    Response process(String[] args) throws InvalidRequestException;
    Response process(Request request);
}
