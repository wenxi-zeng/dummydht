package elastic;

import commonmodels.Indexable;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import util.SimpleLog;

import java.util.Arrays;

public enum ElasticCommand {

    INITIALIZE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ElasticCommand.INITIALIZE.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().initialize();
            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage("Initialized");
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.INITIALIZE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    DESTROY {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ElasticCommand.DESTROY.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.deleteInstance();
            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage("Finished deconstruction");
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.DESTROY.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    READ {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(ElasticCommand.READ.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            Indexable node = LookupTable.getInstance().read(request.getAttachment());
            String result = "Found " + request.getAttachment() + " on:\n" + node.toString();
            SimpleLog.i(result);

            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.READ.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
        }
    },

    WRITE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(ElasticCommand.WRITE.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            Indexable node = LookupTable.getInstance().write(request.getAttachment());
            String result = "Write " + request.getAttachment() + " to:\n" + node.toString();
            SimpleLog.i(result);

            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.WRITE.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
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

            return new Request().withHeader(ElasticCommand.ADDNODE.name())
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
            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.ADDNODE.name() + " %s:%s %s";
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

            return new Request().withHeader(ElasticCommand.REMOVENODE.name())
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
            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.REMOVENODE.name() + " %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }
    },

    MOVEBUCKET{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 4)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            String[] address1 = args[1].split(":");
            String[] address2 = args[2].split(":");

            if (address1.length != 2 || address2.length != 2) {
                throw new InvalidRequestException("Invalid ip format. Try: " + getHelpString());
            }

            String attachment = args[1] + " " + args[2] + " " + args[3];
            return new Request().withHeader(ElasticCommand.MOVEBUCKET.name())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String result;

            String[] args = request.getAttachment().split(" ");
            BucketNode bucketNode = new BucketNode(Integer.valueOf(args[2]));

            String[] address1 = args[0].split(":");
            String[] address2 = args[1].split(":");

            PhysicalNode from = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            PhysicalNode to = new PhysicalNode(address2[0], Integer.valueOf(address2[1]));
            LookupTable.getInstance().moveBucket(bucketNode, from , to);

            result = "Bucket moved";
            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.MOVEBUCKET.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    EXPAND {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 1)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(ElasticCommand.EXPAND.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().expand();
            String result = "Table expanded";

            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.EXPAND.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    SHRINK {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 1)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(ElasticCommand.SHRINK.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().shrink();
            String result = "Table shrunk";

            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.SHRINK.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    LISTPHYSICALNODES {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ElasticCommand.LISTPHYSICALNODES.name());
        }

        @Override
        public Response execute(Request request) {
            String result = LookupTable.getInstance().listPhysicalNodes();
            SimpleLog.i(result);

            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.LISTPHYSICALNODES.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    PRINTLOOKUPTABLE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ElasticCommand.PRINTLOOKUPTABLE.name());
        }

        @Override
        public Response execute(Request request) {
            String result = LookupTable.getInstance().toString();
            SimpleLog.i(result);

            return new Response().withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.PRINTLOOKUPTABLE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    };

    public abstract Request convertToRequest(String[] args) throws InvalidRequestException;
    public abstract Response execute(Request request);
    public abstract String getParameterizedString();
    public abstract String getHelpString();
}
