package ceph;

import commonmodels.Terminal;

public class CephTerminal implements Terminal {
    @Override
    public void initialize() {
        CephCommand.INITIALIZE.execute(null);
    }

    @Override
    public void destroy() {
        CephCommand.DESTROY.execute(null);
    }

    @Override
    public void printInfo() {
        System.out.println("\nAvailable commands:\n" +
                "read <filename>\n" +
                "write <filename>\n" +
                "addNode <cluster id> <ip>:<port>\n" +
                "removeNode <ip>:<port>\n" +
                "changeWeight <delta weight> <ip>:<port>\n" +
                "listPhysicalNodes\n" +
                "printClusterMap\n");
    }

    @Override
    public void execute(String[] args) {
        CephCommand cmd = CephCommand.valueOf(args[0].toUpperCase());
        cmd.execute(args);
    }
}
