package entries;

import ceph.CephTerminal;
import commonmodels.Terminal;
import elastic.ElasticTerminal;
import ring.RingTerminal;

import java.util.Scanner;

public class SingleNodeClient {
    public static void main(String args[]) {
        System.out.println("Select DHT type:\n" +
                "1. Ring\n" +
                "2. Elastic\n" +
                "3. Ceph\n");
        Scanner in = new Scanner(System.in);
        int type = in.nextInt();
        in.nextLine();

        Terminal terminal;
        if (type == 1) {
            terminal = new RingTerminal();
        }
        else if (type == 2) {
            terminal = new ElasticTerminal();
        }
        else if (type == 3) {
            terminal = new CephTerminal();
        }
        else {
            System.out.println("Unknown type\n");
            return;
        }

        terminal.initialize();
        while (true){
            terminal.printInfo();
            String cmdLine[] = in.nextLine().split("\\s+");

            try {
                terminal.process(cmdLine);
            }
            catch (Exception e) {
                System.out.println("Command not found");
                e.printStackTrace();
            }
        }
    }

}
