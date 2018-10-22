package entries;

import models.Indexable;
import models.LookupTable;
import models.PhysicalNode;
import util.SimpleLog;

public enum Command {
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

    INCREASELOAD{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: increaseLoad <ip> <port>");
                return;
            }

            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(args[1]);
            pnode.setPort(Integer.valueOf(args[2]));
            LookupTable.getInstance().increaseLoad(pnode);
        }
    },

    DECREASELOAD{
        public void execute(String[] args) {
            if (args.length != 3) {
                SimpleLog.i("Wrong arguments. Try: decreaseLoad <ip> <port>");
                return;
            }

            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(args[1]);
            pnode.setPort(Integer.valueOf(args[2]));
            LookupTable.getInstance().decreaseLoad(pnode);
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
