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
import loadmanagement.GlobalLoadInfoBroker;
import loadmanagement.LoadInfo;
import util.SimpleLog;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum CommonCommand implements Command {

    START {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.START.name())
                    .withReceiver(args[1]);
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
            return new Request().withHeader(CommonCommand.STOP.name())
                    .withReceiver(args[1]);
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
            Request request = new Request().withHeader(CommonCommand.STATUS.name());
            if (args.length > 1) request.setReceiver(args[1]);
            return request;
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
            return CommonCommand.RECEIVED.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    UPDATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.UPDATE.name());
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
                Object attachment = request.getLargeAttachment();

                SimpleLog.v("Attachment: ====================================\n" + attachment);
                Response response = new Response(request).withStatus(Response.STATUS_SUCCESS);
                try {
                    if (attachment instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Request> delta = (List<Request>) attachment;
                        for (Request r : delta) {
                            SimpleLog.i("Apply delta: " + r);
                            DataNodeDaemon.getInstance().getDataNodeServer().processCommand(r);
                        }
                    } else if (attachment instanceof Request) {
                        Request r = (Request) attachment;
                        SimpleLog.i("Apply delta: " + r);
                        DataNodeDaemon.getInstance().getDataNodeServer().processCommand(r);
                    } else {
                        DataNodeDaemon.getInstance().getDataNodeServer().updateTable(request.getLargeAttachment());
                    }
                } catch (Exception e) {
                    response.withStatus(Response.STATUS_FAILED).withMessage(e.getMessage());
                }

                return response;
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

    LOADHANDSHAKE{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            return new Request().withHeader(CommonCommand.LOADHANDSHAKE.name());
        }

        @Override
        public Response execute(Request request) {
            DecentralizedLoadInfoBroker.getInstance().update((LoadInfo) request.getLargeAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS);
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.LOADHANDSHAKE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    PROPAGATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(CommonCommand.PROPAGATE.name());
        }

        @Override
        public Response execute(Request request) {
            DataNodeDaemon.getInstance().propagateTable();
            return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage("Table propagated.");
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.PROPAGATE.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
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

            return new Request().withHeader(CommonCommand.ADDNODE.name())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            Request followupRequest = new Request()
                    .withHeader(CommonCommand.START.name())
                    .withReceiver(request.getAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS)
                    .withAttachment(followupRequest);
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.ADDNODE.name() + " %s:%s %s";
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
            Request followupRequest = new Request()
                    .withHeader(CommonCommand.STOP.name())
                    .withReceiver(request.getAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS)
                    .withAttachment(followupRequest);
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

    UPDATELOAD{
        @Override
        public Request convertToRequest(String[] args) throws InvalidRequestException {
            return new Request().withHeader(CommonCommand.UPDATELOAD.name());
        }

        @Override
        public Response execute(Request request) {
            GlobalLoadInfoBroker.getInstance().update((LoadInfo) request.getLargeAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS);
        }

        @Override
        public String getParameterizedString() {
            return CommonCommand.UPDATELOAD.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
