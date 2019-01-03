package commonmodels;

import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import entries.DataNodeDaemon;
import filemanagement.FileTransferManager;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum CommonCommand implements Command {

    START {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.START.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            try {
                DataNodeDaemon.getInstance().startDataNodeServer();
                result = "Node started";
                return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
            } catch (Exception e) {
                result = e.getMessage();
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.START.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    STOP {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.STOP.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            try {
                DataNodeDaemon.getInstance().stopDataNodeServer();
                result = "Node stopped";

                return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
            } catch (Exception e) {
                result = e.getMessage();
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.STOP.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    STATUS {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.STATUS.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (DataNodeDaemon.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (DataNodeDaemon.getInstance().getDataNodeServer() == null){
                result = "Data node not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                return DataNodeDaemon.getInstance().getDataNodeServer().getMembersStatus();
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.STATUS.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    FETCH {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.FETCH.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (DataNodeDaemon.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (DataNodeDaemon.getInstance().getDataNodeServer() == null){
                result = "Data node not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                Object table = DataNodeDaemon.getInstance().getDataNodeServer().getDataNodeTable();
                return new Response(request)
                        .withStatus(Response.STATUS_SUCCESS)
                        .withAttachment(table);
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.FETCH.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    TRANSFER {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 4) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request()
                    .withHeader(CommonCommand.TRANSFER.name())
                    .withSender(args[0])
                    .withReceiver(args[1])
                    .withAttachment(args[2]);
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (DataNodeDaemon.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (DataNodeDaemon.getInstance().getDataNodeServer() == null){
                result = "Data node not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                Pattern pattern = Pattern.compile(",");
                List<Integer> buckets = pattern.splitAsStream(request.getAttachment())
                        .map(Integer::valueOf)
                        .collect(Collectors.toList());

                result = FileTransferManager.getInstance().transfer(
                            buckets,
                            new PhysicalNode(request.getSender()),
                            new PhysicalNode(request.getReceiver())
                        );

                return new Response(request)
                        .withStatus(Response.STATUS_SUCCESS)
                        .withAttachment(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.TRANSFER.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    COPY {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 4) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request()
                    .withHeader(CommonCommand.COPY.name())
                    .withSender(args[0])
                    .withReceiver(args[1])
                    .withAttachment(args[2]);
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (DataNodeDaemon.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (DataNodeDaemon.getInstance().getDataNodeServer() == null){
                result = "Data node not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                Pattern pattern = Pattern.compile(",");
                List<Integer> buckets = pattern.splitAsStream(request.getAttachment())
                        .map(Integer::valueOf)
                        .collect(Collectors.toList());

                result = FileTransferManager.getInstance().copy(
                        buckets,
                        new PhysicalNode(request.getSender()),
                        new PhysicalNode(request.getReceiver())
                );

                return new Response(request)
                        .withStatus(Response.STATUS_SUCCESS)
                        .withAttachment(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.COPY.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    RECEIVED {
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            if (args.length != 4) {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request()
                    .withHeader(CommonCommand.RECEIVED.name())
                    .withSender(args[0])
                    .withReceiver(args[1])
                    .withAttachment(args[2]);
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (DataNodeDaemon.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (DataNodeDaemon.getInstance().getDataNodeServer() == null){
                result = "Data node not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                Pattern pattern = Pattern.compile(",");
                List<Integer> buckets = pattern.splitAsStream(request.getAttachment())
                        .map(Integer::valueOf)
                        .collect(Collectors.toList());

                result = FileTransferManager.getInstance().copy(
                        buckets,
                        new PhysicalNode(request.getSender()),
                        new PhysicalNode(request.getReceiver())
                );

                return new Response(request)
                        .withStatus(Response.STATUS_SUCCESS)
                        .withAttachment(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.RECEIVED.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    PROPAGATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.PROPAGATE.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (DataNodeDaemon.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (DataNodeDaemon.getInstance().getDataNodeServer() == null){
                result = "Data node not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                return DataNodeDaemon.getInstance().getDataNodeServer().getMembersStatus();
            }
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.STATUS.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
