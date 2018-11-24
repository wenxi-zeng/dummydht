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
                "read <filename>\n" +
                "write <filename>\n" +
                "addNode <ip> <port>\n" +
                "removeNode <ip> <port>\n" +
                "increaseLoad <ip> <port>\n" +
                "decreaseLoad <ip> <port>\n" +
                "listPhysicalNodes\n" +
                "printLookupTable\n");
    }

    @Override
    public void execute(String[] args) {
        RingCommand cmd = RingCommand.valueOf(args[0].toUpperCase());
        cmd.execute(args);
    }

}
