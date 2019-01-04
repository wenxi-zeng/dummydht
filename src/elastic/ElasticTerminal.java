package elastic;

import commands.ElasticCommand;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

public class ElasticTerminal implements Terminal {

    @Override
    public void initialize() {
        ElasticCommand.INITIALIZE.execute(null);
    }

    @Override
    public void destroy() {
        ElasticCommand.DESTROY.execute(null);
    }

    @Override
    public void printInfo() {
        System.out.println("\nAvailable commands:\n" +
                ElasticCommand.READ.getHelpString() + "\n" +
                ElasticCommand.WRITE.getHelpString() + "\n" +
                ElasticCommand.ADDNODE.getHelpString() + "\n" +
                ElasticCommand.REMOVENODE.getHelpString() + "\n" +
                ElasticCommand.MOVEBUCKET.getHelpString() + "\n" +
                ElasticCommand.EXPAND.getHelpString() + "\n" +
                ElasticCommand.SHRINK.getHelpString() + "\n" +
                ElasticCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                ElasticCommand.PRINTLOOKUPTABLE.getHelpString() + "\n");
    }

    @Override
    public long getEpoch() {
        return LookupTable.getInstance().getEpoch();
    }

    @Override
    public Response process(String[] args) throws InvalidRequestException {
        try {
            ElasticCommand cmd = ElasticCommand.valueOf(args[0].toUpperCase());
            Request request = cmd.convertToRequest(args);
            return cmd.execute(request);
        }
        catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Command " + args[0] + " not found");
        }
    }

    @Override
    public Response process(Request request) {
        ElasticCommand cmd = ElasticCommand.valueOf(request.getHeader());
        return cmd.execute(request);
    }

    @Override
    public Request translate(String[] args) throws InvalidRequestException {
        try {
            ElasticCommand cmd = ElasticCommand.valueOf(args[0].toUpperCase());
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
}

