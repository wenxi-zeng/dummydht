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
    },

    DESTROY {
        public void execute(String[] args) {
            ClusterMap.deleteInstance();
        }
    },

    READ {
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: read <filename>");
                return;
            }

            Clusterable node = ClusterMap.getInstance().read(args[1]);
            SimpleLog.i("Found " + args[1] + " on:\n" + node.toString());
        }
    },

    WRITE{
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: write <filename>");
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
    },

    ADDNODE{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: addNode <cluster id> <ip>:<port>");
                return;
            }

            String clusterId = args[1];
            String[] address1 = args[2].split(":");
            if (address1.length != 2) {
                SimpleLog.i("Invalid ip format. Try: addNode <cluster id> <ip>:<port>");
                return;
            }
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));

            ClusterMap.getInstance().addNode(clusterId, pnode);
        }
    },

    REMOVENODE{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: removeNode <ip> <port>");
                return;
            }

            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(args[1]);
            pnode.setPort(Integer.valueOf(args[2]));
            ClusterMap.getInstance().removeNode(pnode);
        }
    },

    CHANGEWEIGHT{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: changeWeight <delta weight> <ip>:<port>");
                return;
            }

            float deltaWeight = Float.valueOf(args[1]);
            String[] address1 = args[2].split(":");
            if (address1.length != 2) {
                SimpleLog.i("Invalid ip format. Try: changeWeight <delta weight> <ip>:<port>");
                return;
            }
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));

            ClusterMap.getInstance().changeWeight(pnode, deltaWeight);
        }
    },

    LISTPHYSICALNODES {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: listPhysicalNodes");
                return;
            }

            SimpleLog.i(ClusterMap.getInstance().listPhysicalNodes());
        }
    },

    PRINTCLUSTERMAP {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: printClusterMap");
                return;
            }

            SimpleLog.i(ClusterMap.getInstance().toString());
        }
    };

    public abstract void execute(String[] args);
}
