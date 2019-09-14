package commands;

import commonmodels.Command;
import commonmodels.PhysicalNode;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import filemanagement.DummyFile;
import filemanagement.FileBucket;
import filemanagement.LocalFileManager;
import org.apache.commons.lang3.StringUtils;
import ring.LookupTable;
import util.Config;
import util.MathX;
import util.SimpleLog;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
            return RingCommand.LOOKUP.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "<filename>");
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

            return new Request().withHeader(RingCommand.READ.name())
                    .withEpoch(LookupTable.getInstance().getEpoch())
                    .withAttachment(attachment);
        }

        @Override
        public Response execute(Request request) {
            String[] file = request.getAttachment().split(" ");
            int hash = MathX.positiveHash(file[0].hashCode()) % Config.getInstance().getNumberOfHashSlots();
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

            if (request.getEpoch() < LookupTable.getInstance().getEpoch()) {
                @SuppressWarnings("unchecked")
                List<Request> delta = (List<Request>)LookupTable.getInstance().getDeltaSupplier().get();
                delta.sort(Comparator.comparingLong(Request::getTimestamp));
                if (delta.size() < 1 || request.getEpoch() < delta.get(0).getTimestamp()) {
                    response.setAttachment(LookupTable.getInstance());
                }
                else {
                    List attachment = delta.stream()
                            .filter(d -> d.getTimestamp() > request.getEpoch())
                            .collect(Collectors.toList());
                    if (attachment.size() > 0)
                        response.setAttachment(attachment);
                }
            }

            return response;
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.READ.name() + " %s %s";
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

            return new Request().withHeader(RingCommand.WRITE.name())
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

            if (shouldReplicate && request.getEpoch() < LookupTable.getInstance().getEpoch()) {
                @SuppressWarnings("unchecked")
                List<Request> delta = (List<Request>)LookupTable.getInstance().getDeltaSupplier().get();
                delta.sort(Comparator.comparingLong(Request::getTimestamp));
                if (delta.size() < 1 || request.getEpoch() < delta.get(0).getTimestamp()) {
                    response.setAttachment(LookupTable.getInstance());
                }
                else {
                    List attachment = delta.stream()
                            .filter(d -> d.getTimestamp() > request.getEpoch())
                            .collect(Collectors.toList());
                    if (attachment.size() > 0)
                        response.setAttachment(attachment);
                }
            }

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
                String buckets = StringUtils.join(LookupTable.getInstance().getSpareBuckets(), ',');
                attachment = args[1] + " " + buckets;
            }
            else if (args.length == 3) {
                attachment = args[1] + " " + args[2];
            }
            else  {
                throw new InvalidRequestException("Wrong arguments. Try: " + getHelpString());
            }

            return new Request().withHeader(RingCommand.ADDNODE.name())
                    .withAttachment(attachment)
                    .withReceiver(args[1]);
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

            int[] deltaHash = LookupTable.getInstance().randomIncreaseRange(new PhysicalNode(args[1]));
            return new Request().withHeader(RingCommand.INCREASELOAD.name())
                    .withAttachments(args[1], StringUtils.join(deltaHash, ','));
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
                LookupTable.getInstance().increaseLoad(pnode);
            }
            else {
                String[] deltaHash = args[1].split(",");
                LookupTable.getInstance().increaseLoad(pnode, Arrays.stream(deltaHash).mapToInt(Integer::parseInt).toArray());
            }

            result = "Load increased";
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS)
                    .withMessage(result);
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

            int[] deltaHash = LookupTable.getInstance().randomDecreaseRange(new PhysicalNode(args[1]));
            return new Request().withHeader(RingCommand.DECREASELOAD.name())
                    .withAttachments(args[1], StringUtils.join(deltaHash, ','));
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
                LookupTable.getInstance().decreaseLoad(pnode);
            }
            else {
                String[] deltaHash = args[1].split(",");
                LookupTable.getInstance().decreaseLoad(pnode, Arrays.stream(deltaHash).mapToInt(Integer::parseInt).toArray());
            }

            result = "Load decreased";
            return new Response(request)
                    .withStatus(Response.STATUS_SUCCESS)
                    .withMessage(result);
        }

        @Override
        public String getParameterizedString() {
            return RingCommand.DECREASELOAD.name() + " %s:%s";
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
            return RingCommand.LISTPHYSICALNODES.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "[ip:port]");
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
            return RingCommand.PRINTLOOKUPTABLE.name() + " %s";
        }

        @Override
        public String getHelpString() {
            return String.format(getParameterizedString(), "[ip:port]");
        }

    },

    UPDATE {
        @Override
        public Request convertToRequest(String[] args) {
            return new Request().withHeader(RingCommand.UPDATE.name());
        }

        @Override
        public Response execute(Request request) {
            Object attachment = request.getLargeAttachment();

            // SimpleLog.v("Attachment: ====================================\n" + attachment);
            Response response = new Response(request).withStatus(Response.STATUS_SUCCESS);
            try {
                if (attachment instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Request> delta = (List<Request>) attachment;
                    for (Request r : delta) {
                        // SimpleLog.i("Apply delta: " + r);
                        if (r.getTimestamp() >= LookupTable.getInstance().getEpoch()) {
                            RingCommand cmd = RingCommand.valueOf(r.getHeader());
                            response = cmd.execute(r);
                            LookupTable.getInstance().setEpoch(r.getTimestamp());
                        }
                    }
                } else if (attachment instanceof Request) {
                    Request r = (Request) attachment;
                    // SimpleLog.i("Apply delta: " + r);
                    if (r.getTimestamp() >= LookupTable.getInstance().getEpoch()) {
                        RingCommand cmd = RingCommand.valueOf(r.getHeader());
                        response = cmd.execute(r);
                        LookupTable.getInstance().setEpoch(r.getTimestamp());
                    }
                } else {
                    String result = LookupTable.getInstance().updateTable(request.getLargeAttachment());
                    response.setMessage(result);
                }
            } catch (Exception e) {
                response.withStatus(Response.STATUS_FAILED).withMessage(e.getMessage());
            }

            return response.withAttachment(LookupTable.getInstance().getTable());
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
