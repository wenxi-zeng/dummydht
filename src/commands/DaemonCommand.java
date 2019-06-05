package commands;

import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import entries.DataNodeDaemon;
import filemanagement.FileBucket;
import filemanagement.FileTransferManager;
import loadmanagement.DecentralizedLoadInfoBroker;
import loadmanagement.LoadInfo;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum DaemonCommand implements Command {

    START {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(DaemonCommand.START.name());
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
            return DaemonCommand.START.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    STOP {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(DaemonCommand.STOP.name());
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
            return DaemonCommand.STOP.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    STATUS {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(DaemonCommand.STATUS.name());
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
            return DaemonCommand.STATUS.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    FETCH {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(DaemonCommand.FETCH.name());
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
            return DaemonCommand.FETCH.name();
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
                    .withHeader(DaemonCommand.TRANSFER.name())
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
            return DaemonCommand.TRANSFER.name() + " %s:%s %s:%s %s";
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
                    .withHeader(DaemonCommand.COPY.name())
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
            return DaemonCommand.COPY.name() + " %s:%s %s:%s %s";
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
                    .withHeader(DaemonCommand.RECEIVED.name())
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
            else if (request.getLargeAttachment() == null){
                result = "No file buckets found";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                //noinspection unchecked
                float transferElapsed = FileTransferManager.getInstance().received(
                        (List<FileBucket>) request.getLargeAttachment(),
                        new PhysicalNode(request.getSender()),
                        new PhysicalNode(request.getReceiver())
                );
                result = "Total file migration time: " + transferElapsed;
                request.addProcessTime(transferElapsed);

                return new Response(request)
                        .withStatus(Response.STATUS_SUCCESS)
                        .withAttachment(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return DaemonCommand.RECEIVED.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    UPDATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(DaemonCommand.UPDATE.name());
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
                DataNodeDaemon.getInstance().getDataNodeServer().updateTable(request.getLargeAttachment());
                return new Response(request).withStatus(Response.STATUS_SUCCESS);
            }
        }

        @Override
        public String getParameterizedString() {
            return DaemonCommand.STATUS.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    LOADHANDSHAKE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            return new Request().withHeader(DaemonCommand.LOADHANDSHAKE.name());
        }

        @Override
        public Response execute(Request request) {
            DecentralizedLoadInfoBroker.getInstance().update((LoadInfo) request.getLargeAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS);
        }

        @Override
        public String getParameterizedString() {
            return DaemonCommand.LOADHANDSHAKE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
