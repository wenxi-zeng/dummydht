package commands;

import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import entries.Proxy;
import filemanagement.FileBucket;
import filemanagement.FileTransferManager;
import loadmanagement.GlobalLoadInfoManager;
import loadmanagement.LoadInfo;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum ProxyCommand implements Command {

    START {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ProxyCommand.START.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            try {
                Proxy.getInstance().startDataNodeServer();
                result = "Node started";
                return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
            } catch (Exception e) {
                result = e.getMessage();
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.START.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    STOP {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ProxyCommand.STOP.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            try {
                Proxy.getInstance().stopDataNodeServer();
                result = "Node stopped";

                return new Response(request).withStatus(Response.STATUS_SUCCESS).withMessage(result);
            } catch (Exception e) {
                result = e.getMessage();
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.STOP.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    STATUS {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ProxyCommand.STATUS.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (Proxy.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (Proxy.getInstance().getDataNodeServer() == null){
                result = "Proxy not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                return Proxy.getInstance().getDataNodeServer().getMembersStatus();
            }
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.STATUS.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    },

    FETCH {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ProxyCommand.FETCH.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (Proxy.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (Proxy.getInstance().getDataNodeServer() == null){
                result = "Proxy not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                Object table = Proxy.getInstance().getDataNodeServer().getDataNodeTable();
                return new Response(request)
                        .withStatus(Response.STATUS_SUCCESS)
                        .withAttachment(table);
            }
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.FETCH.name();
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
                    .withHeader(ProxyCommand.TRANSFER.name())
                    .withSender(args[0])
                    .withReceiver(args[1])
                    .withAttachment(args[2]);
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (Proxy.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (Proxy.getInstance().getDataNodeServer() == null){
                result = "Proxy not started";
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
            return ProxyCommand.TRANSFER.name() + " %s:%s %s:%s %s";
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
                    .withHeader(ProxyCommand.COPY.name())
                    .withSender(args[0])
                    .withReceiver(args[1])
                    .withAttachment(args[2]);
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (Proxy.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (Proxy.getInstance().getDataNodeServer() == null){
                result = "Proxy not started";
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
            return ProxyCommand.COPY.name() + " %s:%s %s:%s %s";
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
                    .withHeader(ProxyCommand.RECEIVED.name())
                    .withSender(args[0])
                    .withReceiver(args[1])
                    .withAttachment(args[2]);
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (Proxy.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (Proxy.getInstance().getDataNodeServer() == null){
                result = "Proxy not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (request.getLargeAttachment() == null){
                result = "No file buckets found";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                //noinspection unchecked
                result = FileTransferManager.getInstance().received(
                        (List<FileBucket>) request.getLargeAttachment(),
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
            return ProxyCommand.RECEIVED.name() + " %s:%s %s:%s %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<from ip>", "<port>", "<to ip>", "<port>", "<bucket>");
        }
    },

    PROPAGATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(ProxyCommand.PROPAGATE.name());
        }

        @Override
        public Response execute(Request request) {
            String result;
            if (Proxy.getInstance() == null) {
                result = "Daemon not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else if (Proxy.getInstance().getDataNodeServer() == null){
                result = "Proxy not started";
                return new Response(request).withStatus(Response.STATUS_FAILED).withMessage(result);
            }
            else {
                return Proxy.getInstance().getDataNodeServer().getMembersStatus();
            }
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.PROPAGATE.name();
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

            return new Request().withHeader(ProxyCommand.ADDNODE.name())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            Request followupRequest = new Request()
                                .withHeader(ProxyCommand.START.name())
                                .withReceiver(request.getAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS)
                    .withAttachment(followupRequest);
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.ADDNODE.name() + " %s:%s %s";
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
                    .withHeader(ProxyCommand.STOP.name())
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
            return new Request().withHeader(ProxyCommand.UPDATELOAD.name());
        }

        @Override
        public Response execute(Request request) {
            GlobalLoadInfoManager.getInstance().update((LoadInfo) request.getLargeAttachment());
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS);
        }

        @Override
        public String getParameterizedString() {
            return ProxyCommand.UPDATELOAD.name();
        }

        @Override
        public String getHelpString() {
            return getParameterizedString();
        }
    }
}
