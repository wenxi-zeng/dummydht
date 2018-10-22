package entries;

import models.LookupTable;

import java.util.Scanner;

import static util.Config.CONFIG_CEPH;
import static util.Config.CONFIG_ELASTIC;
import static util.Config.CONFIG_RING;

public class SingleNodeClient {
    public static void main(String args[]) {
        System.out.println("Select DHT type:\n" +
                "1. Ring\n" +
                "2. Elastic\n" +
                "3. Ceph\n");
        Scanner in = new Scanner(System.in);
        int type = in.nextInt();
        in.nextLine();

        LookupTable table = LookupTable.getInstance();
        if (type == 1) {
            table.initialize(CONFIG_RING);
        }
        else if (type == 2) {
            table.initialize(CONFIG_ELASTIC);
        }
        else if (type == 3) {
            table.initialize(CONFIG_CEPH);
        }
        else {
            System.out.println("Unknown type\n");
            return;
        }

        while (true){
            System.out.println("\nAvailable commands:\n" +
                    "read <filename>\n" +
                    "write <filename>\n" +
                    "addNode <ip> <port>\n" +
                    "removeNode <ip> <port>\n" +
                    "increaseLoad <ip> <port>\n" +
                    "decreaseLoad <ip> <port>\n");

            String cmdLine[] = in.nextLine().split("\\s+");

            try {
                Command cmd = Command.valueOf(cmdLine[0].toUpperCase());
                cmd.execute(cmdLine);
            }
            catch (Exception e) {
                System.out.println("Command not found");
            }
        }
    }

}
