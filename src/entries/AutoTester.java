package entries;

import util.Config;
import util.ResourcesLoader;

import java.io.*;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class AutoTester {

    private final Timer timer;
    private final int startTag;
    private final int endTag;
    private final int delay;
    private final int step;
    private int currentTag;
    private int currentRequests;
    private int suffix;
    private final TestCallBack callBack = AutoTester.this::start;
    private static final String[] MODE_SCHEME = {"centralized-ring-1000", "centralized-elastic-100", "centralized-ceph-100"};
//    private static final String[] MODE_SCHEME = {"distributed-ring-1000", "distributed-elastic-100"};
    private static final int MODE_SCHEME_TAG_DIFF = 100000;
    private int modeCounter;
    private String mode;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Invalid Arguments. Try: dummydht.jar -a <startTag> <endTag> <delay> [step]");
            return;
        }
        int startTag = Integer.valueOf(args[0]);
        int endTag = Integer.valueOf(args[1]);
        int delay = Integer.valueOf(args[2]);
        int step = args.length == 4 ? Integer.valueOf(args[3]) : 0;
        AutoTester tester = new AutoTester(startTag, endTag, delay, step);
        tester.start();
    }

    public AutoTester(int startTag, int endTag, int delay, int step) {
        timer = new Timer();
        this.startTag = startTag;
        this.endTag = endTag;
        this.delay = delay;
        this.step = step;
        this.currentRequests = -1;
        this.currentTag = startTag;
        this.suffix = 0;
        this.modeCounter = 0;
        this.mode = "centralized";
    }

    private void start() {
        try {
            Properties prop = new Properties();
            String propPath = ResourcesLoader.getRelativePathToRes("config.properties");
            InputStream in = new FileInputStream(propPath);
            prop.load(in);
            String[] currentModeScheme = MODE_SCHEME[modeCounter].split("-");
            String mode = currentModeScheme[0];
            String scheme = currentModeScheme[1];
            int pvRatio = Integer.valueOf(currentModeScheme[2]);
            if (currentTag - modeCounter * MODE_SCHEME_TAG_DIFF > endTag) {
                System.out.println("All tests finished!");
                System.exit(0);
                return;
            }
            if (currentRequests < 0)
                currentRequests = Integer.valueOf(prop.getProperty(Config.PROPERTY_NUMBER_OF_REQUESTS));
            int physicalNodes = Integer.valueOf(prop.getProperty(Config.PROPERTY_INIT_NUMBER_OF_ACTIVE_NODES));
            prop.setProperty(Config.PROPERTY_HASH_SLOTS, String.valueOf(physicalNodes * pvRatio));
            prop.setProperty(Config.PROPERTY_NUMBER_OF_PLACEMENT_GROUPS, String.valueOf(physicalNodes * pvRatio));
            prop.setProperty(Config.PROPERTY_MODE, mode);
            prop.setProperty(Config.PROPERTY_SCHEME, scheme);
            prop.setProperty(Config.PROPERTY_TRIAL_TAG, String.valueOf(currentTag));
            prop.setProperty(Config.PROPERTY_NUMBER_OF_REQUESTS, String.valueOf(currentRequests));
            prop.store(new FileOutputStream(propPath), null);
            System.out.println("Tag updated...");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Task scheduled...");
        timer.schedule(new TestTask(callBack), delay * 60 * 1000);
    }

    class TestTask extends TimerTask {

        private final TestCallBack testCallBack;

        public TestTask(TestCallBack callBack) {
            this.testCallBack = callBack;
        }

        @Override
        public void run() {
            try {
                String[] cmd = new String[]{"/bin/sh", ResourcesLoader.getRelativeFileName(ScriptGenerator.FILE_UPDATE_ALL)};
                await(cmd);
                System.out.println("Files updated...");
                // Thread.sleep(3 * 60 * 1000);

                cmd = new String[]{"/bin/sh", ResourcesLoader.getRelativeFileName(ScriptGenerator.FILE_START_ALL)};
                await(cmd);
                System.out.println("Daemons started...");

                if (mode.equals("centralized")) {
                    cmd = new String[]{"/bin/sh", ResourcesLoader.getRelativeFileName(ScriptGenerator.FILE_START_PROXY)};
                    await(cmd);
                    System.out.println("Proxy started...");
                    Thread.sleep(5 * 1000);
                }
                else {
                    cmd = new String[]{"/bin/sh", ResourcesLoader.getRelativeFileName(ScriptGenerator.FILE_START_SEEDS)};
                    await(cmd);
                    System.out.println("Seed daemons started...");
                    Thread.sleep(5 * 1000);
                    cmd = new String[]{"java", "-jar", "dummydht.jar", "-n", "addnode", Config.getInstance().getSeeds().get(0)};
                    await(cmd);
                    System.out.println("Bootstrapping...");
                    Thread.sleep(10 * 1000);
                }

                System.out.println("Launch client...");
                // RegularClient.main(new String[]{ "-r", ResourcesLoader.getParentDirOfProgramPath() + File.separator + "test" + File.separator + "full5.txt" });
                String requestFilePath = ResourcesLoader.getParentDirOfProgramPath() + File.separator + "test" + File.separator + "requests" + currentRequests + "s" + suffix + ".txt";
                File requestFile = new File(requestFilePath);
                if (requestFile.exists()) {
                    cmd = new String[]{"java", "-jar", "dummydht.jar", "-c", "-s", requestFilePath};
                }
                else {
                    cmd = new String[]{"java", "-jar", "dummydht.jar", "-c", "-f", ResourcesLoader.getParentDirOfProgramPath() + File.separator + "test" + File.separator + "full5.txt", requestFilePath};
                    await(cmd);
                    cmd = new String[]{"java", "-jar", "dummydht.jar", "-c", "-s", requestFilePath};
                }
                await(cmd);
                //Runtime.getRuntime().exec("java -jar dummydht.jar -c -r ~/test/full5.txt").waitFor();

                cmd = new String[]{"/bin/sh", ResourcesLoader.getRelativeFileName(ScriptGenerator.FILE_STOP_ALL_BUT_CLIENT)};
                await(cmd);
                System.out.println("Test[" + currentTag +  "] done!");

                modeCounter = (modeCounter + 1) % MODE_SCHEME.length;
                if (modeCounter == 0) {
                    currentTag -= (MODE_SCHEME.length - 1) * MODE_SCHEME_TAG_DIFF;
                    if (step > 0) {
                        currentTag += (step / 10);
                        currentRequests += step;
                    } else {
                        currentTag++;
                        suffix++;
                    }
                }
                else {
                    currentTag += MODE_SCHEME_TAG_DIFF;
                }
                if (testCallBack != null)
                    testCallBack.onTestFinished();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void await(String[] cmd) {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            try {
                Process process = pb.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null)
                    System.out.println(line);
                process.waitFor();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    interface TestCallBack {
        void onTestFinished();
    }
}
