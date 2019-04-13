package commonmodels;

import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

public interface Command {
    Request convertToRequest(String[] args) throws InvalidRequestException;
    Response execute(Request request);
    String getParameterizedString();
    String getHelpString();
}
