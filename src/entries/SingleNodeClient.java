package entries;

import commonmodels.Terminal;
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
            terminal = new RingTerminal();
        }
        else if (type == 3) {
            terminal = new RingTerminal();
        }
        else {
            System.out.println("Unknown type\n");
            return;
        }

        while (true){
            terminal.printInfo();
            String cmdLine[] = in.nextLine().split("\\s+");

            try {
                terminal.execute(cmdLine);
            }
            catch (Exception e) {
                System.out.println("Command not found");
            }
        }
    }

}
