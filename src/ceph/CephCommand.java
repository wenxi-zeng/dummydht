package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.List;

public enum CephCommand {
    INITIALIZE {
        public void execute(String[] args) {
            ClusterMap.getInstance();
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
            ClusterMap.deleteInstance();
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

            Clusterable node = ClusterMap.getInstance().read(args[1]);
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

            List<Clusterable> nodes = ClusterMap.getInstance().write(args[1]);

            StringBuilder result = new StringBuilder();
            if (nodes != null) {
                for (Clusterable node : nodes) {
                    result.append(node.toString()).append('\n');
                }

                SimpleLog.i("Write " + args[1] + " to:\n" + result.toString());
            }
            else {
                SimpleLog.i("Failed to write " + args[1]);
            }
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
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            String clusterId = args[1];
            String[] address1 = args[2].split(":");
            if (address1.length != 2) {
                SimpleLog.i("Invalid ip format. Try: " + getHelpString());
                return;
            }
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));

            ClusterMap.getInstance().addNode(clusterId, pnode);
        }

        @Override
        public String getParameterizedString() {
            return "addNode %s %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<cluster id>", "<ip>", "<port>");
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
            ClusterMap.getInstance().removeNode(pnode);
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

    CHANGEWEIGHT{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            float deltaWeight = Float.valueOf(args[1]);
            String[] address1 = args[2].split(":");
            if (address1.length != 2) {
                SimpleLog.i("Invalid ip format. Try: " + getHelpString());
                return;
            }
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));

            ClusterMap.getInstance().changeWeight(pnode, deltaWeight);
        }

        @Override
        public String getParameterizedString() {
            return "changeWeight %s %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<delta weight>", "<ip>", "<port>");
        }
    },

    LISTPHYSICALNODES {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            SimpleLog.i(ClusterMap.getInstance().listPhysicalNodes());
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

    PRINTCLUSTERMAP {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: " + getHelpString());
                return;
            }

            SimpleLog.i(ClusterMap.getInstance().toString());
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
