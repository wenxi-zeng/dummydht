package entries;

import ceph.CephTerminal;
import commonmodels.Terminal;
import commonmodels.transport.Response;
import elastic.ElasticTerminal;
import ring.RingTerminal;
import util.Config;

import java.util.Scanner;

public class SingleNodeClient {
    public static void main(String args[]) {
        String scheme = Config.getInstance().getScheme();
        Config.getInstance().setStandalone(true);
        Terminal terminal;

        switch (scheme) {
            case Config.SCHEME_RING:
                terminal = new RingTerminal();
                break;
            case Config.SCHEME_ELASTIC:
                terminal = new ElasticTerminal();
                break;
            case Config.SCHEME_CEPH:
                terminal = new CephTerminal();
                break;
            default:
                System.out.println("Unknown type\n");
                return;
        }

        Scanner in = new Scanner(System.in);
        terminal.initialize();
        while (true){
            terminal.printInfo();
            String cmdLine[] = in.nextLine().split("\\s+");

            try {
                Response response = terminal.process(cmdLine);
                //System.out.println(response.toString());
            }
            catch (Exception e) {
                //System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
