package ring;

import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

public class RingTerminal implements Terminal {

    @Override
    public void initialize() {
        RingCommand.INITIALIZE.execute(null);
    }

    @Override
    public void destroy() {
        RingCommand.DESTROY.execute(null);
    }

    @Override
    public void printInfo() {
        System.out.println("\nAvailable commands:\n" +
                RingCommand.READ.getHelpString() + "\n" +
                RingCommand.WRITE.getHelpString() + "\n" +
                RingCommand.ADDNODE.getHelpString() + "\n" +
                RingCommand.REMOVENODE.getHelpString() + "\n" +
                RingCommand.INCREASELOAD.getHelpString() + "\n" +
                RingCommand.DECREASELOAD.getHelpString() + "\n" +
                RingCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                RingCommand.PRINTLOOKUPTABLE.getHelpString() + "\n");
    }

    @Override
    public Response process(String[] args) throws InvalidRequestException {
        try {
            RingCommand cmd = RingCommand.valueOf(args[0].toUpperCase());
            Request request = cmd.convertToRequest(args);
            return cmd.execute(request);
        }
        catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Command " + args[0] + " not found");
        }
    }

    @Override
    public Response process(Request request) {
        RingCommand cmd = RingCommand.valueOf(request.getHeader());
        return cmd.execute(request);
    }

}
