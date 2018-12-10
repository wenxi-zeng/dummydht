package ring;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

public enum RingCommand {
    INITIALIZE {
        public void execute(String[] args) {
            LookupTable.getInstance();
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
        public void execute(String[] args) {
            LookupTable.deleteInstance();
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
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            Indexable node = LookupTable.getInstance().read(args[1]);
            SimpleLog.i("Found " + args[1] + " on:\n" + node.toString());
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
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            Indexable node = LookupTable.getInstance().write(args[1]);
            SimpleLog.i("Write " + args[1] + " to:\n" + node.toString());
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
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[1]);
            pnode.setPort(Integer.valueOf(address[2]));
            LookupTable.getInstance().addNode(pnode);
        }

        @Override
        public String getParameterizedString() {
            return "addNode %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    REMOVENODE{
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[1]);
            pnode.setPort(Integer.valueOf(address[2]));
            LookupTable.getInstance().removeNode(pnode);
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
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[1]);
            pnode.setPort(Integer.valueOf(address[2]));
            LookupTable.getInstance().increaseLoad(pnode);
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
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[1]);
            pnode.setPort(Integer.valueOf(address[2]));
            LookupTable.getInstance().decreaseLoad(pnode);
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
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            SimpleLog.i(LookupTable.getInstance().listPhysicalNodes());
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
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            SimpleLog.i(LookupTable.getInstance().toString());
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

    public abstract void execute(String[] args);
    public abstract String getParameterizedString();
    public abstract String getHelpString();
}
