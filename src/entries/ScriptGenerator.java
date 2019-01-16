package entries;

import util.Config;
import util.ResourcesLoader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class ScriptGenerator {

    private final static String FILE_START_ALL = "start-all.sh";

    private final static String FILE_STOP_ALL = "stop-all.sh";

    private final static String FILE_CONFIG_ALL = "config-all.sh";

    private final static String FILE_UPDATE_ALL = "update-all.sh";

    private final static String FILE_MKDIR_ALL = "mkdir-all.sh";

    private final static String FILE_AUTH_ALL = "auth-all.sh";

    private static String SELF;

    public static void main(String args[]) {
        SELF = getAddress();
        generateStartAll();
        generateStopAll();
        generateUpdateAll();
        generateConfigAll();
        generateMakeDirAll();
        generateAuthorizeAll();
    }

    private static void generateStartAll() {
        String filename = ResourcesLoader.getRelativeFileName(FILE_START_ALL);
        Config config = Config.getInstance();
        int startPort = config.getStartPort();
        int portRange = config.getPortRange();
        StringBuilder stringBuilder = new StringBuilder();

        for (String node : config.getNodes()) {
            StringBuilder command = new StringBuilder();
            for (int i = 0; i < portRange; i++) {
                command.append("java -jar ").append(File.separator).append("root").append(File.separator).append("dummydht").append(File.separator).append("dummydht-daemon.jar ")
                        .append(startPort + i)
                        .append(" &").append('\n');
            }

            if (node.contains(SELF))
                stringBuilder.append(command);
            else
                stringBuilder.append("sshpass -p alien1 ssh -tt root@").append(node).append(" << EOF").append('\n').append(command).append("exit").append('\n').append("EOF").append('\n');
        }

        try (PrintStream out = new PrintStream(createFile(filename))) {
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void generateStopAll() {
        StringBuilder stringBuilder = new StringBuilder();

        for (String node : Config.getInstance().getNodes()) {
            if (node.contains(SELF))
                stringBuilder.append("pgrep java | xargs kill -9").append('\n');
            else
                stringBuilder.append("sshpass -p alien1 ssh root@").append(node).append(" \"pgrep java | xargs kill -9\"").append('\n');
        }

        String filename = ResourcesLoader.getRelativeFileName(FILE_STOP_ALL);
        try (PrintStream out = new PrintStream(createFile(filename))) {
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void generateUpdateAll() {
        StringBuilder stringBuilder = new StringBuilder();
        String configFile = ResourcesLoader.getRelativeFileName(null);

        for (String node : Config.getInstance().getNodes()) {
            if (!node.contains(SELF))
                stringBuilder.append("sshpass -p alien1 scp -rp ").append(configFile).append(" root@")
                        .append(node).append(":").append(File.separator)
                        .append("root").append("\n");
        }

        String filename = ResourcesLoader.getRelativeFileName(FILE_UPDATE_ALL);
        try (PrintStream out = new PrintStream(createFile(filename))) {
            //yum -y install sshpass
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void generateConfigAll() {
        StringBuilder stringBuilder = new StringBuilder();
        String configFile = ResourcesLoader.getRelativeFileName("res" + File.separator + "config.properties");

        for (String node : Config.getInstance().getNodes()) {
            if (!node.contains(SELF))
                stringBuilder.append("sshpass -p alien1 scp -r root@")
                        .append(node).append(":")
                        .append(configFile).append(" ")
                        .append(configFile).append("\n");
        }

        String filename = ResourcesLoader.getRelativeFileName(FILE_CONFIG_ALL);
        try (PrintStream out = new PrintStream(createFile(filename))) {
            //yum -y install sshpass
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void generateMakeDirAll() {
        StringBuilder stringBuilder = new StringBuilder();
        String configFile = ResourcesLoader.getRelativeFileName(null);

        for (String node : Config.getInstance().getNodes()) {
            if (!node.contains(SELF))
                stringBuilder.append("sshpass -p alien1 ssh root@")
                        .append(node)
                        .append(" \"mkdir -m 777 /root/dummydht\"").append("\n");
        }

        String filename = ResourcesLoader.getRelativeFileName(FILE_MKDIR_ALL);
        try (PrintStream out = new PrintStream(createFile(filename))) {
            //yum -y install sshpass
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static File createFile(String filename) {
        File file = new File(filename);
        try {
            file.createNewFile();
            file.setReadable(true, false);
            file.setExecutable(true, false);
            file.setWritable(true, false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return file;
    }

    private static void generateAuthorizeAll() {
        String filename = ResourcesLoader.getRelativeFileName(FILE_AUTH_ALL);
        Config config = Config.getInstance();
        int startPort = config.getStartPort();
        int portRange = config.getPortRange();
        StringBuilder stringBuilder = new StringBuilder();

        for (String node : config.getNodes()) {
            StringBuilder command = new StringBuilder();
            for (int i = 0; i < portRange; i++) {
                command.append("firewall-cmd --zone=public --permanent --add-port=").append(startPort + i).append("/tcp\n")
                        .append("firewall-cmd --zone=public --permanent --add-port=").append(startPort + i).append("/udp\n");
            }

            command.append("systemctl restart firewalld\n");
            if (node.contains(SELF))
                stringBuilder.append(command);
            else
                stringBuilder.append("sshpass -p alien1 ssh -tt root@").append(node).append(" << EOF").append('\n').append(command).append("exit").append('\n').append("EOF").append('\n');
        }

        try (PrintStream out = new PrintStream(createFile(filename))) {
            out.println(stringBuilder.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String getAddress() {
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (UnknownHostException | SocketException e) {
            e.printStackTrace();
        }

        return null;
    }
}
