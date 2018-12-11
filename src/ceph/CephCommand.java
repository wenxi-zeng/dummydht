package ceph;

import commonmodels.Clusterable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.List;

public enum CephCommand {
    BOOTSTRAP {
        public String execute(String[] args) {
            ClusterMap.getInstance().bootstrap();
            return "Finished bootstrap";
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
        public String execute(String[] args) {
            ClusterMap.getInstance().initialize();
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
            ClusterMap.deleteInstance();
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

            Clusterable node = ClusterMap.getInstance().read(args[1]);
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
            String execResult;

            if (args.length != 2) {
                execResult = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(execResult);
                return execResult;
            }

            List<Clusterable> nodes = ClusterMap.getInstance().write(args[1]);

            StringBuilder result = new StringBuilder();
            if (nodes != null) {
                for (Clusterable node : nodes) {
                    result.append(node.toString()).append('\n');
                }

                execResult = "Write " + args[1] + " to:\n" + result.toString();
                SimpleLog.i(execResult);
            }
            else {
                execResult = "Failed to write " + args[1];
                SimpleLog.i(execResult);
            }

            return execResult;
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

            if (args.length != 3) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            String clusterId = args[1];
            String[] address1 = args[2].split(":");
            if (address1.length != 2) {
                result = "Invalid ip format. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            ClusterMap.getInstance().addNode(clusterId, pnode);
            result = "Node added";

            return result;
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
            ClusterMap.getInstance().removeNode(pnode);

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

    CHANGEWEIGHT{
        public String execute(String[] args) {
            String result;

            if (args.length != 3) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            float deltaWeight = Float.valueOf(args[1]);
            String[] address1 = args[2].split(":");
            if (address1.length != 2) {
                result = "Invalid ip format. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));

            ClusterMap.getInstance().changeWeight(pnode, deltaWeight);

            result = "Weight changed";
            return result;
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
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            result = ClusterMap.getInstance().listPhysicalNodes();
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

    PRINTCLUSTERMAP {
        public String execute(String[] args) {
            String result;

            if (args.length != 1) {
                result = "Wrong arguments. Try: " + getHelpString();
                SimpleLog.i(result);
                return result;
            }

            result = ClusterMap.getInstance().toString();
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
