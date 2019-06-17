package commands;

import ceph.ClusterMap;
import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import filemanagement.DummyFile;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.util.List;

public enum CephCommand implements Command {

    INITIALIZE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CephCommand.INITIALIZE.name());
        }

        @Override
        public Response execute(Request request) {
            ClusterMap.getInstance().initialize(request == null ? null : request.getAttachment());
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Initialized");
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.INITIALIZE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    DESTROY {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CephCommand.DESTROY.name());
        }

        @Override
        public Response execute(Request request) {
            ClusterMap.deleteInstance();
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Finished deconstruction");
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.DESTROY.name();
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

            return new Request().withHeader(CephCommand.READ.name())
                    .withEpoch(ClusterMap.getInstance().getEpoch())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String[] file = request.getAttachment().split(" ");
            int hash = MathX.positiveHash(file[0].hashCode()) % Config.getInstance().getNumberOfPlacementGroups();
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

            if (request.getEpoch() < ClusterMap.getInstance().getEpoch())
                response.setAttachment(ClusterMap.getInstance());

            return response;
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.READ.name() + " %s";
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

            return new Request().withHeader(CephCommand.WRITE.name())
                    .withEpoch(ClusterMap.getInstance().getEpoch())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            boolean shouldReplicate = request.getEpoch() >= 0;
            DummyFile file = new DummyFile(request.getAttachment());
            FileBucket fileBucket = ClusterMap.getInstance().write(
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

            if (shouldReplicate && request.getEpoch() < ClusterMap.getInstance().getEpoch())
                response.setAttachment(ClusterMap.getInstance());

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

            return new Request().withHeader(CephCommand.LOOKUP.name())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            List<PhysicalNode> pnodes = ClusterMap.getInstance().lookup(request.getAttachment());
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withAttachment(pnodes);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.LOOKUP.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
        }

    },

    ADDNODE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 3) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            if (!args[1].contains(":")) {
                throw new InvalidRequestException("Invalid ip format. Try: " + getHelpString());
            }

            String attachment = args[1] + " " + args[2];
            return new Request().withHeader(CephCommand.ADDNODE.name())
                    .withAttachment(attachment)
                    .withReceiver(args[1]);
        }

        @Override
        public Response execute(Request request) {
            String result;

            String[] args = request.getAttachment().split(" ");
            String clusterId = args[1];
            String[] address1 = args[0].split(":");

            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            ClusterMap.getInstance().addNode(clusterId, pnode);
            result = "Node added";

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.ADDNODE.name() + " %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>", "<cluster id>");
        }

    },

    REMOVENODE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 2)  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(CephCommand.REMOVENODE.name())
                    .withAttachment(args[1])
                    .withFollowup(args[1])
                    .withReceiver(args[1]);
        }

        @Override
        public Response execute(Request request) {
            String result;

            String[] address = request.getAttachment().split(":");
            PhysicalNode pnode = new PhysicalNode();
            pnode.setAddress(address[0]);
            pnode.setPort(Integer.valueOf(address[1]));
            ClusterMap.getInstance().removeNode(pnode);

            result = "Node removed";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.REMOVENODE.name() + " %s:%s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>");
        }

    },

    CHANGEWEIGHT{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 3) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            if (!args[1].contains(":")) {
                throw new InvalidRequestException("Invalid ip format. Try: " + getHelpString());
            }

            String attachment = args[1] + " " + args[2];

            return new Request().withHeader(CephCommand.CHANGEWEIGHT.name())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String result;
            String[] args = request.getAttachment().split(" ");

            float deltaWeight = Float.valueOf(args[1]);
            String[] address1 = args[0].split(":");
            PhysicalNode pnode = new PhysicalNode(address1[0], Integer.valueOf(address1[1]));
            ClusterMap.getInstance().changeWeight(pnode, deltaWeight);

            result = "Weight changed";
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.CHANGEWEIGHT.name() + " %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<ip>", "<port>", "<delta weight>");
        }
    },

    LISTPHYSICALNODES {
        @Override
        public Request convertToRequest(String[] args) {
            Request request = new Request().withHeader(CephCommand.LISTPHYSICALNODES.name());
            if (args.length == 2)
                request.setReceiver(args[1]);

            return request;
        }

        @Override
        public Response execute(Request request) {
            String result = ClusterMap.getInstance().listPhysicalNodes();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.LISTPHYSICALNODES.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "[ip:port]");
        }
    },

    PRINTCLUSTERMAP {
        @Override
        public Request convertToRequest(String[] args) {
            Request request = new Request().withHeader(CephCommand.PRINTCLUSTERMAP.name());
            if (args.length == 2)
                request.setReceiver(args[1]);

            return request;
        }

        @Override
        public Response execute(Request request) {
            String result = ClusterMap.getInstance().toString();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.PRINTCLUSTERMAP.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "[ip:port]");
        }
    },

    UPDATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CephCommand.UPDATE.name());
        }

        @Override
        public Response execute(Request request) {
            String result = ClusterMap.getInstance().updateTable(request.getLargeAttachment());
            if (result.equals(ClusterMap.UPDATE_STATUS_DONE)) {
                ClusterMap.getInstance().scheduleLoadBalancing();
            }
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.UPDATE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    PROPAGATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CephCommand.PROPAGATE.name());
        }

        @Override
        public Response execute(Request request) {
            ClusterMap.getInstance().propagateTableChanges();
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Table propagated.");
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.PROPAGATE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
