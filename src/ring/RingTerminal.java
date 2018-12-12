package ring;

import commonmodels.Terminal;

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
    public String execute(String[] args) {
        RingCommand cmd = RingCommand.valueOf(args[0].toUpperCase());
        return cmd.execute(args);
    }

}
