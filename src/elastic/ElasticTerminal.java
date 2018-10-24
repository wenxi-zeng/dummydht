package elastic;

import commonmodels.Terminal;

public class ElasticTerminal implements Terminal {

    @Override
    public void initialize() {
        ElasticCommand.INITIALIZE.execute(null);
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
        ElasticCommand cmd = ElasticCommand.valueOf(args[0].toUpperCase());
        cmd.execute(args);
    }

}

