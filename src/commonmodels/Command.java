package commonmodels;

import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

public interface Command {
    public abstract Request convertToRequest(String[] args) throws InvalidRequestException;
    public abstract Response execute(Request request);
    public abstract String getParameterizedString();
    public abstract String getHelpString();
}
