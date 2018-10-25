package ceph;

import commonmodels.Terminal;

public class CephTerminal implements Terminal {
    @Override
    public void initialize() {
        CephCommand.INITIALIZE.execute(null);
    }

    @Override
    public void printInfo() {
        System.out.println("\nAvailable commands:\n" +
                "read <filename>\n" +
                "write <filename>\n" +
                "addNode <ip> <port>\n" +
                "removeNode <ip> <port>\n" +
                "moveBucket <bucket> <from ip>:<port> <to ip>:<port>\n" +
                "listPhysicalNodes\n" +
                "printLookupTable\n");
    }

    @Override
    public void execute(String[] args) {
        CephCommand cmd = CephCommand.valueOf(args[0].toUpperCase());
        cmd.execute(args);
    }
}
