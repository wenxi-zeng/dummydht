package elastic;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

public enum ElasticCommand {

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

            if (args.length != 2) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[1]);
            pnode.setPort(Integer.valueOf(address[2]));
            LookupTable.getInstance().addNode(pnode);

            result = "Node added";
            return result;
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
        public String execute(String[] args) {
            String result;

            if (args.length != 2) {
                result = "Wrong arguments. Try: removeNode " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String[] address = args[1].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[1]);
            pnode.setPort(Integer.valueOf(address[2]));
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

    MOVEBUCKET{
        public String execute(String[] args) {
            String result;

            if (args.length != 4) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            BucketNode bucketNode = new BucketNode(Integer.valueOf(args[1]));

            String[] address1 = args[2].split(":");
            String[] address2 = args[3].split(":");

            if (address1.length != 2 || address2.length != 2) {
                result = "Invalid ip format. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            PhysicalNode from = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            PhysicalNode to = new PhysicalNode(address2[0], Integer.valueOf(address2[1]));
            LookupTable.getInstance().moveBucket(bucketNode, from , to);

            result = "Bucket moved";
            return result;
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
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            LookupTable.getInstance().expand();
            result = "Table expanded";
            return result;
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
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            LookupTable.getInstance().shrink();

            result = "Table shrunk";
            return result;
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
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " +getHelpString();
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
