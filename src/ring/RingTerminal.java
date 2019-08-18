package ring;

import commands.RingCommand;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import util.URIHelper;

import java.util.ArrayList;
import java.util.List;

public class RingTerminal implements Terminal {

    private List<String> tableChangeCommand;

    public RingTerminal() {
        tableChangeCommand = new ArrayList<>();
        tableChangeCommand.add(RingCommand.ADDNODE.name());
        tableChangeCommand.add(RingCommand.REMOVENODE.name());
        tableChangeCommand.add(RingCommand.INCREASELOAD.name());
        tableChangeCommand.add(RingCommand.DECREASELOAD.name());
    }

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
    public long getEpoch() {
        return LookupTable.getInstance().getEpoch();
    }

    @Override
    public Response process(String[] args) throws InvalidRequestException {
        try {
            RingCommand cmd = RingCommand.valueOf(args[0].toUpperCase());
            URIHelper.verifyAddress(args);
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

    @Override
    public Request translate(String[] args) throws InvalidRequestException {
        try {
            RingCommand cmd = RingCommand.valueOf(args[0].toUpperCase());
            URIHelper.verifyAddress(args);
            return cmd.convertToRequest(args);
        }
        catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Command " + args[0] + " not found");
        }
    }

    @Override
    public Request translate(String command) throws InvalidRequestException {
        return translate(command.split(" "));
    }

    @Override
    public boolean isRequestCauseTableUpdates(Request request) {
        return tableChangeCommand.contains(request.getHeader());
    }

}
