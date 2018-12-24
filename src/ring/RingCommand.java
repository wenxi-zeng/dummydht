package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.Arrays;

public enum RingCommand {

    INITIALIZE {
        public String execute(String[] args) {
            LookupTable.getInstance().initialize();
            return "Finished initialization";
        }

        @Override
        public String getParameterizedString() {
            return "INITIALIZE";
        }

        @Override
        public String getHelpString() {
            return "INITIALIZE";
        }
    },

    DESTROY {
        public String execute(String[] args) {
            LookupTable.deleteInstance();
            return "Finished deconstruction";
        }

        @Override
        public String getParameterizedString() {
            return "DESTROY";
        }

        @Override
        public String getHelpString() {
            return "DESTROY";
        }
    },

    READ {
        public String execute(String[] args) {
            String result;

            if (args.length != 2) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            Indexable node = LookupTable.getInstance().read(args[1]);

            result = "Found " + args[1] + " on:\n" + node.toString();
            SimpleLog.i(result);

            return result;
        }

        @Override
        public String getParameterizedString() {
            return "read %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
        }

    },

    WRITE{
        public String execute(String[] args) {
            String result;

            if (args.length != 2) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            Indexable node = LookupTable.getInstance().write(args[1]);
            result = "Write " + args[1] + " to:\n" + node.toString();
            SimpleLog.i(result);

            return result;
        }

        @Override
        public String getParameterizedString() {
            return "write %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
        }

    },

    ADDNODE{
        public String execute(String[] args) {
            String result;

            if (args.length != 2 && args.length != 3) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));

            if (args.length == 2) {
                LookupTable.getInstance().addNode(pnode);
            }
            else {
                String[] hashVal = args[2].split(",");
                LookupTable.getInstance().addNode(pnode, Arrays.stream(hashVal).mapToInt(Integer::parseInt).toArray());
            }

            result = "Node added";
            return result;
        }

        @Override
        public String getParameterizedString() {
            return "addNode %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>", "[bucket1,bucket2,...]");
        }

    },

    REMOVENODE{
        public String execute(String[] args) {
            String result;

            if (args.length != 2) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            LookupTable.getInstance().removeNode(pnode);

            result = "Node removed";
            return result;
        }

        @Override
        public String getParameterizedString() {
            return "removeNode %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    INCREASELOAD{
        public String execute(String[] args) {
            String result;

            if (args.length != 2) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            LookupTable.getInstance().increaseLoad(pnode);

            result = "Load increased";
            return result;
        }

        @Override
        public String getParameterizedString() {
            return "increaseLoad %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    DECREASELOAD{
        public String execute(String[] args) {
            String result;

            if (args.length != 2) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            LookupTable.getInstance().decreaseLoad(pnode);

            result = "Load decreased";
            return result;
        }

        @Override
        public String getParameterizedString() {
            return "decreaseLoad %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    LISTPHYSICALNODES {
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            result = LookupTable.getInstance().listPhysicalNodes();
            SimpleLog.i(result);

            return result;
        }

        @Override
        public String getParameterizedString() {
            return "listPhysicalNodes";
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }

    },

    PRINTLOOKUPTABLE {
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            result = LookupTable.getInstance().toString();
            SimpleLog.i(result);

            return result;
        }

        @Override
        public String getParameterizedString() {
            return "printLookupTable";
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }

    };

    public abstract String execute(String[] args);
    public abstract String getParameterizedString();
    public abstract String getHelpString();
}
