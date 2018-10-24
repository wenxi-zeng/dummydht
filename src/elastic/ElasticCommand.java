package elastic;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import util.SimpleLog;

public enum ElasticCommand {
    INITIALIZE {
        public void execute(String[] args) {
            LookupTable.getInstance();
        }
    },

    READ {
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: read <filename>");
                return;
            }

            Indexable node = LookupTable.getInstance().read(args[1]);
            SimpleLog.i("Found " + args[1] + " on:\n" + node.toString());
        }
    },

    WRITE{
        public void execute(String[] args) {
            if (args.length != 2) {
                SimpleLog.i("Wrong arguments. Try: write <filename>");
                return;
            }

            Indexable node = LookupTable.getInstance().write(args[1]);
            SimpleLog.i("Write " + args[1] + " to:\n" + node.toString());
        }
    },

    ADDNODE{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: addNode <ip> <port>");
                return;
            }

            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(args[1]);
            pnode.setPort(Integer.valueOf(args[2]));
            LookupTable.getInstance().addNode(pnode);
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
            LookupTable.getInstance().removeNode(pnode);
        }
    },

    MOVEBUCKET{
        public void execute(String[] args) {
            if (args.length != 4) {
                SimpleLog.i("Wrong arguments. Try: moveBucket <bucket> <from ip>:<port> <to ip>:<port>");
                return;
            }

            BucketNode bucketNode = new BucketNode(Integer.valueOf(args[1]));

            String[] address1 = args[2].split(":");
            String[] address2 = args[3].split(":");

            if (address1.length != 2 || address2.length != 2) {
                SimpleLog.i("Invalid address format. Try: moveBucket <bucket> <from ip>:<port> <to ip>:<port>");
                return;
            }

            PhysicalNode from = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            PhysicalNode to = new PhysicalNode(address2[0], Integer.valueOf(address2[1]));
            LookupTable.getInstance().moveBucket(bucketNode, from , to);
        }
    },

    LISTPHYSICALNODES {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: listPhysicalNodes");
                return;
            }

            SimpleLog.i(LookupTable.getInstance().listPhysicalNodes());
        }
    },

    PRINTLOOKUPTABLE {
        public void execute(String[] args) {
            if (args.length != 1) {
                SimpleLog.i("Wrong arguments. Try: printLookupTable");
                return;
            }

            SimpleLog.i(LookupTable.getInstance().toString());
        }
    };

    public abstract void execute(String[] args);
}
