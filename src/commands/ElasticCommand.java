package commands;

import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import elastic.BucketNode;
import elastic.LookupTable;
import filemanagement.DummyFile;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import loadmanagement.LoadInfoManager;
import org.apache.commons.lang3.StringUtils;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.util.Arrays;
import java.util.List;

public enum ElasticCommand implements Command {

    INITIALIZE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ElasticCommand.INITIALIZE.name());
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().initialize();
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Initialized");
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
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Finished deconstruction");
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
            if (args.length > 3) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            String attachment = args[1];
            if (args.length == 3)
                attachment = attachment + " " + args[2];

            return new Request().withHeader(ElasticCommand.READ.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String[] file = request.getAttachment().split(" ");
            int hash = MathX.positiveHash(file[0].hashCode()) % LookupTable.getInstance().getTable().length;
            long filesize = file.length == 2 ? Long.valueOf(file[1]) : -1;
            FileBucket fileBucket = LocalFileManager.getInstance().read(hash, filesize);

            Response response = new Response(request);

            if (fileBucket == null) {
                response.withStatus(Response.STATUS_FAILED)
                        .withMessage("Bucket not found in this node.");
            }
            else {
                response.withStatus(Response.STATUS_SUCCESS)
                        .withMessage(fileBucket.toString());
            }

            if (request.getEpoch() < LookupTable.getInstance().getEpoch())
                response.setAttachment(LookupTable.getInstance().getTable());

            return response;
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.READ.name() + " %s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename> [filesize]");
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

            return new Request().withHeader(ElasticCommand.WRITE.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            boolean shouldReplicate = request.getEpoch() >= 0;
            DummyFile file = new DummyFile(request.getAttachment());
            FileBucket fileBucket = LookupTable.getInstance().write(
                    file,
                    shouldReplicate);

            Response response = new Response(request);
            if (fileBucket.isLocked()) {
                response.withStatus(Response.STATUS_FAILED)
                        .withMessage("Bucket is locked.");
            }
            else {
                response.withStatus(Response.STATUS_SUCCESS)
                        .withMessage(fileBucket.toString());
            }

            if (!shouldReplicate && request.getEpoch() < LookupTable.getInstance().getEpoch())
                response.setAttachment(LookupTable.getInstance().getTable());

            return response;
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.WRITE.name() + " %s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>", "[size]");
        }

    },

    LOOKUP {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(ElasticCommand.LOOKUP.name())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            List<PhysicalNode> pnodes = LookupTable.getInstance().lookup(request.getAttachment());
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withAttachment(pnodes);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.LOOKUP.name() + " %s";
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
                String buckets = StringUtils.join(LookupTable.getInstance().getSpareBuckets(), ',');
                attachment = args[1] + " " + buckets;
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
                String[] hashVal = args[1].split(",");
                LookupTable.getInstance().addNode(pnode, Arrays.stream(hashVal).mapToInt(Integer::parseInt).toArray());
            }

            result = "Node added";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
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
                    .withAttachment(args[1])
                    .withFollowup(args[1]);
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
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
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
            if (args.length !=  1 && args.length != 2)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            Request request = new Request().withHeader(ElasticCommand.EXPAND.name());
            if (args.length == 2)
                request.setReceiver(args[1]);

            return request;
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().expand();
            String result = "Table expanded";

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return ElasticCommand.EXPAND.name() + " %s";
            else
                return ElasticCommand.EXPAND.name();
        }

        @Override
        public String getHelpString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return String.format(getParameterizedString(), "[ip:port]");
            else
                return getParameterizedString();
        }
    },

    SHRINK {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length !=  1 && args.length != 2)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            Request request = new Request().withHeader(ElasticCommand.SHRINK.name());
            if (args.length == 2)
                request.setReceiver(args[1]);

            return request;
        }

        @Override
        public Response execute(Request request) {
            LookupTable.getInstance().shrink();
            String result = "Table shrunk";

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }


        @Override
        public String getParameterizedString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return ElasticCommand.SHRINK.name() + " %s";
            else
                return ElasticCommand.SHRINK.name();
        }

        @Override
        public String getHelpString() {
            if (Config.getInstance().getMode().equals(Config.MODE_DISTRIBUTED))
                return String.format(getParameterizedString(), "[ip:port]");
            else
                return getParameterizedString();
        }
    },

    LISTPHYSICALNODES {
        @Override
        public Request convertToRequest(String[] args) {
            Request request = new Request().withHeader(ElasticCommand.LISTPHYSICALNODES.name());
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
            return ElasticCommand.LISTPHYSICALNODES.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "[ip:port]");
        }
    },

    PRINTLOOKUPTABLE {
        @Override
        public Request convertToRequest(String[] args) {
            Request request = new Request().withHeader(ElasticCommand.PRINTLOOKUPTABLE.name());
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
            return ElasticCommand.PRINTLOOKUPTABLE.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "[ip:port]");
        }
    },

    UPDATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ElasticCommand.UPDATE.name());
        }

        @Override
        public Response execute(Request request) {
            String result = LookupTable.getInstance().updateTable(request.getLargeAttachment());
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return ElasticCommand.UPDATE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
