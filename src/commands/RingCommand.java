package commands;

import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import ring.LookupTable;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.util.Arrays;
import java.util.List;

public enum RingCommand implements Command {

    INITIALIZE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(RingCommand.INITIALIZE.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().initialize();
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Initialized");
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.INITIALIZE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    DESTROY {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(RingCommand.DESTROY.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.deleteInstance();
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Finished deconstruction");
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.DESTROY.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    LOOKUP {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.LOOKUP.name())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            List<PhysicalNode> pnodes = LookupTable.getInstance().lookup(request.getAttachment());
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withAttachment(pnodes);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.READ.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
        }

    },

    READ {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.READ.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            int hash = MathX.positiveHash(request.getAttachment().hashCode()) % Config.getInstance().getNumberOfHashSlots();
            FileBucket fileBucket = LocalFileManager.getInstance().read(hash);

            Response response = new Response(request);

            if (fileBucket == null)
                response.withStatus(Response.STATUS_FAILED)
                        .withMessage("Bucket not found in this node.");
            else
                response.withStatus(Response.STATUS_SUCCESS)
                        .withMessage(fileBucket.toString());

            if (request.getEpoch() < LookupTable.getInstance().getEpoch())
                response.setAttachment(LookupTable.getInstance().getTable());

            return response;
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.READ.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
        }

    },

    WRITE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2 && args.length != 3) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            String attachment = args[1];
            if (args.length == 3)
                attachment += " " + args[2];

            return new Request().withHeader(RingCommand.WRITE.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String[] temp = request.getAttachment().split(" ");

            String filename = temp[0];
            int hash = MathX.positiveHash(filename.hashCode()) % Config.getInstance().getNumberOfHashSlots();
            FileBucket fileBucket;
            if (temp.length == 2) {
                long filesize = Long.valueOf(temp[1]);
                fileBucket = LocalFileManager.getInstance().write(hash, filesize);
            }
            else {
                fileBucket = LocalFileManager.getInstance().write(hash);
            }

            Response response = new Response(request);
            if (fileBucket.isLocked())
                response.withStatus(Response.STATUS_FAILED)
                        .withMessage("Bucket is locked.");
            else
                response.withStatus(Response.STATUS_SUCCESS)
                        .withMessage(fileBucket.toString());

            if (request.getEpoch() < LookupTable.getInstance().getEpoch())
                response.setAttachment(LookupTable.getInstance().getTable());

            return response;
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.WRITE.name() + " %s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>", "[size]");
        }

    },

    ADDNODE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            String attachment;

            if (args.length == 2) {
                attachment = args[1];
            }
            else if (args.length == 3) {
                attachment = args[1] + " " + args[2];
            }
            else  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.ADDNODE.name())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String result;
            String[] args = request.getAttachment().split(" ");

            String[] address = args[0].split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));

            if (args.length == 1) {
                LookupTable.getInstance().addNode(pnode);
            }
            else {
                String[] hashVal = args[2].split(",");
                LookupTable.getInstance().addNode(pnode, Arrays.stream(hashVal).mapToInt(Integer::parseInt).toArray());
            }

            result = "Node added";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.ADDNODE.name() + " %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>", "[bucket1,bucket2,...]");
        }

    },

    REMOVENODE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.REMOVENODE.name())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            String result;

            String[] address = request.getAttachment().split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            LookupTable.getInstance().removeNode(pnode);

            result = "Node removed";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.REMOVENODE.name() + " %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    INCREASELOAD{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.INCREASELOAD.name())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            String result;

            String[] address = request.getAttachment().split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            LookupTable.getInstance().increaseLoad(pnode);

            result = "Load increased";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.INCREASELOAD.name() + " %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    DECREASELOAD{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.DECREASELOAD.name())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            String result;

            String[] address = request.getAttachment().split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            LookupTable.getInstance().decreaseLoad(pnode);

            result = "Load decreased";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.INITIALIZE.name() + " %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    LISTPHYSICALNODES {
        @Override
        public Request convertToRequest(String[] args) {
            Request request = new Request().withHeader(RingCommand.LISTPHYSICALNODES.name());
            if (args.length == 2)
                request.setReceiver(args[1]);

            return request;
        }

        @Override
        public Response execute(Request request) {
            String result = LookupTable.getInstance().listPhysicalNodes();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return RingCommand.LISTPHYSICALNODES.name() + " %s";
            else
                return RingCommand.LISTPHYSICALNODES.name();
        }

        @Override
        public String getHelpString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return String.format(getParameterizedString(), "[ip:port]");
            else
                return getParameterizedString();
        }

    },

    PRINTLOOKUPTABLE {
        @Override
        public Request convertToRequest(String[] args) {
            Request request = new Request().withHeader(RingCommand.PRINTLOOKUPTABLE.name());
            if (args.length == 2)
                request.setReceiver(args[1]);

            return request;
        }

        @Override
        public Response execute(Request request) {
            String result = LookupTable.getInstance().toString();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return RingCommand.PRINTLOOKUPTABLE.name() + " %s";
            else
                return RingCommand.PRINTLOOKUPTABLE.name();
        }

        @Override
        public String getHelpString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return String.format(getParameterizedString(), "[ip:port]");
            else
                return getParameterizedString();
        }

    },

    UPDATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(RingCommand.UPDATE.name());
        }

        @Override
        public Response execute(Request request) {
            String result = LookupTable.getInstance().updateTable(request.getLargeAttachment());
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.UPDATE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
