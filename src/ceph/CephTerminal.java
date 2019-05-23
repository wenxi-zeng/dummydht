package ceph;

import commands.CephCommand;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import util.URIHelper;

public class CephTerminal implements Terminal {

    private String machineAddress;

    public CephTerminal() {
    }

    public CephTerminal(String machineAddress) {
        this.machineAddress = machineAddress;
    }

    @Override
    public void initialize() {
        CephCommand.INITIALIZE.execute(machineAddress == null ? null : new Request().withAttachment(machineAddress));
    }

    @Override
    public void destroy() {
        CephCommand.DESTROY.execute(null);
    }

    @Override
    public void printInfo() {
        System.out.println("\nAvailable commands:\n" +
                CephCommand.READ.getHelpString() + "\n" +
                CephCommand.WRITE.getHelpString() + "\n" +
                CephCommand.ADDNODE.getHelpString() + "\n" +
                CephCommand.REMOVENODE.getHelpString() + "\n" +
                CephCommand.CHANGEWEIGHT.getHelpString() + "\n" +
                CephCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                CephCommand.PRINTCLUSTERMAP.getHelpString() + "\n");
    }

    @Override
    public long getEpoch() {
        return ClusterMap.getInstance().getEpoch();
    }

    @Override
    public Response process(String[] args) throws InvalidRequestException {
        try {
            CephCommand cmd = CephCommand.valueOf(args[0].toUpperCase());
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
        CephCommand cmd = CephCommand.valueOf(request.getHeader());
        return cmd.execute(request);
    }

    @Override
    public Request translate(String[] args) throws InvalidRequestException {
        try {
            CephCommand cmd = CephCommand.valueOf(args[0].toUpperCase());
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
}
