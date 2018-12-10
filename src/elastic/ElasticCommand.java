package elastic;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

public enum ElasticCommand {

    BOOTSTRAP {
        public void execute(String[] args) {
            LookupTable.getInstance().bootstrap();
        }

        @Override
        public String getParameterizedString() {
            return "BOOTSTRAP";
        }

        @Override
        public String getHelpString() {
            return "BOOTSTRAP";
        }
    },

    INITIALIZE {
        public void execute(String[] args) {
            LookupTable.getInstance().initialize();
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
                SimpleLog.i("Wrong arguments. Try: removeNode " + getHelpString());
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

    MOVEBUCKET{
        public void execute(String[] args) {
            if (args.length != 4) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            BucketNode bucketNode = new BucketNode(Integer.valueOf(args[1]));

            String[] address1 = args[2].split(":");
            String[] address2 = args[3].split(":");

            if (address1.length != 2 || address2.length != 2) {
                SimpleLog.i("Invalid ip format. Try: " + getHelpString());
                return;
            }

            PhysicalNode from = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            PhysicalNode to = new PhysicalNode(address2[0], Integer.valueOf(address2[1]));
            LookupTable.getInstance().moveBucket(bucketNode, from , to);
        }

        @Override
        public String getParameterizedString() {
            return "moveBucket %s %s:%s %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<bucket>", "<from ip>", "<port>", "<to ip>", "<port>");
        }
    },

    EXPAND {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            LookupTable.getInstance().expand();
        }

        @Override
        public String getParameterizedString() {
            return "expand";
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    SHRINK {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            LookupTable.getInstance().shrink();
        }

        @Override
        public String getParameterizedString() {
            return "shrink";
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    LISTPHYSICALNODES {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " +getHelpString());
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
