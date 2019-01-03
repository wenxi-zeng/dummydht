package ceph;

import commonmodels.Clusterable;
import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
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
            ClusterMap.getInstance().initialize();
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
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(CephCommand.READ.name())
                    .withEpoch(ClusterMap.getInstance().getEpoch())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            Clusterable node = ClusterMap.getInstance().read(request.getAttachment());
            String result = "Found " + request.getAttachment() + " on:\n" + node.toString();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
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
            if (args.length != 2) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(CephCommand.WRITE.name())
                    .withEpoch(ClusterMap.getInstance().getEpoch())
                    .withAttachment(args[1]);
        }

        @Override
        public Response execute(Request request) {
            String execResult;

            List<Clusterable> nodes = ClusterMap.getInstance().write(request.getAttachment());

            StringBuilder result = new StringBuilder();
            if (nodes != null) {
                for (Clusterable node : nodes) {
                    result.append(node.toString()).append('\n');
                }

                execResult = "Write " + request.getAttachment() + " to:\n" + result.toString();
                SimpleLog.i(execResult);
            }
            else {
                execResult = "Failed to write " + request.getAttachment();
                SimpleLog.i(execResult);
            }

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(execResult);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.WRITE.name() + " %s";
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
                    .withAttachment(attachment);
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
                    .withAttachment(args[1]);
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
            return new Request().withHeader(CephCommand.LISTPHYSICALNODES.name());
        }

        @Override
        public Response execute(Request request) {
            String result = ClusterMap.getInstance().listPhysicalNodes();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.LISTPHYSICALNODES.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    PRINTCLUSTERMAP {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CephCommand.PRINTCLUSTERMAP.name());
        }

        @Override
        public Response execute(Request request) {
            String result = ClusterMap.getInstance().toString();
            SimpleLog.i(result);

            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return CephCommand.PRINTCLUSTERMAP.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    };
}
