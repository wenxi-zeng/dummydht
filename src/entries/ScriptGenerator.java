package entries;

import util.Config;
import util.ResourcesLoader;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class ScriptGenerator {

    private final static String FILE_START_ALL = "start-all.sh";

    private final static String FILE_STOP_ALL = "stop-all.sh";

    private final static String FILE_CONFIG_ALL = "config-all.sh";

    public static void main(String args[]) {
        generateStartAll();
        generateStopAll();
        generateConfigAll();
    }

    private static void generateStartAll() {
        String filename = ResourcesLoader.getRelativeFileName(FILE_START_ALL);
        Config config = Config.getInstance();
        int startPort = config.getStartPort();
        int portRange = config.getPortRange();
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < portRange; i++) {
            stringBuilder
                    .append("java -jar dummydht-daemon.jar ")
                    .append(startPort + i)
                    .append(" &").append('\n');
        }

        try (PrintStream out = new PrintStream(filename)) {
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void generateStopAll() {
        String filename = ResourcesLoader.getRelativeFileName(FILE_STOP_ALL);
        try (PrintStream out = new PrintStream(filename)) {
            out.println("pgrep java | xargs kill -9");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void generateConfigAll() {
        String filename = ResourcesLoader.getRelativeFileName(FILE_CONFIG_ALL);
        try (PrintStream out = new PrintStream(filename)) {
            //yum -y install sshpass
            out.println("sshpass -p \"password\" scp -r user@example.com:/some/remote/path /some/local/path\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
