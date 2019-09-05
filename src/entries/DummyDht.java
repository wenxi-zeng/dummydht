package entries;

import java.util.Arrays;

public class DummyDht {

    public static void main(String[] args){
        if (args.length == 0) {
            System.out.println("Wrong arguments");
            return;
        }

        String driver = args[0];
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        if (driver.equals("-client") || driver.equals("-c")) {
            RegularClient.main(subArgs);
        }
        else if (driver.equals("-daemon") || driver.equals("-d")) {
            DataNodeDaemon.main(subArgs);
        }
        else if (driver.equals("-logserver") || driver.equals("-l")) {
            UDPLogServer.main(subArgs);
        }
        else if (driver.equals("-nodetool") || driver.equals("-n")) {
            DataNodeTool.main(subArgs);
        }
        else if (driver.equals("-proxy") || driver.equals("-p")) {
            Proxy.main(subArgs);
        }
        else if (driver.equals("-statserver") || driver.equals("-s")) {
            StatServer.main(subArgs);
        }
        else if (driver.equals("-scriptgen") || driver.equals("-S")) {
            ScriptGenerator.main(subArgs);
        }
        else if (driver.equals("-auto") || driver.equals("-a")) {
            AutoTester.main(subArgs);
        }
    }

}
