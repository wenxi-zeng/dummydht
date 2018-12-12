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
                CephCommand.READ.getHelpString() + "\n" +
                CephCommand.WRITE.getHelpString() + "\n" +
                CephCommand.ADDNODE.getHelpString() + "\n" +
                CephCommand.REMOVENODE.getHelpString() + "\n" +
                CephCommand.CHANGEWEIGHT.getHelpString() + "\n" +
                CephCommand.LISTPHYSICALNODES.getHelpString() + "\n" +
                CephCommand.PRINTCLUSTERMAP.getHelpString() + "\n");
    }

    @Override
    public String execute(String[] args) {
        CephCommand cmd = CephCommand.valueOf(args[0].toUpperCase());
        return cmd.execute(args);
    }
}
