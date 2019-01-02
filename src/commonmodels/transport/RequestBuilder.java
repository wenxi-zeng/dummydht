package commonmodels.transport;

public class RequestBuilder {

    public static abstract class Builder {
        private Request requestTemp;

        public Builder(String header) {
            requestTemp = new Request().withHeader(header);
        }

        public Builder sender(String sender) {
            requestTemp.setSender(sender);
            return this;
        }

        public Builder receiver(String receiver) {
            requestTemp.setReceiver(receiver);
            return this;
        }

        public Builder forwardTo(String forwardTo) {
            requestTemp.setForwardTo(forwardTo);
            return this;
        }

        public Builder attachment(String attachment) {
            requestTemp.setAttachment(attachment);
            return this;
        }

        public Builder epoch(long epoch) {
            requestTemp.setEpoch(epoch);
            return this;
        }

        public Request build() {
            return new Request()
                    .withHeader(requestTemp.getHeader())
                    .withAttachment(requestTemp.getAttachment())
                    .withForwardTo(requestTemp.getForwardTo())
                    .withReceiver(requestTemp.getReceiver())
                    .withSender(requestTemp.getSender())
                    .withEpoch(requestTemp.getEpoch());
        }
    }

    public static class InitializeRquestBuilder extends Builder {
        public InitializeRquestBuilder() {
            super(Request.HEADER_INITIALIZE);
        }
    }

    public static class DestroyRquestBuilder extends Builder {
        public DestroyRquestBuilder() {
            super(Request.HEADER_DESTROY);
        }
    }

    public static class ReadRquestBuilder extends Builder {
        public ReadRquestBuilder() {
            super(Request.HEADER_READ);
        }
    }

    public static class WriteRquestBuilder extends Builder {
        public WriteRquestBuilder() {
            super(Request.HEADER_WRITE);
        }
    }

    public static class AddNodeRquestBuilder extends Builder {
        public AddNodeRquestBuilder() {
            super(Request.HEADER_ADDNODE);
        }
    }

    public static class RemoveNodeRquestBuilder extends Builder {
        public RemoveNodeRquestBuilder() {
            super(Request.HEADER_REMOVENODE);
        }
    }

    public static class IncreaseLoadRquestBuilder extends Builder {
        public IncreaseLoadRquestBuilder() {
            super(Request.HEADER_INCREASELOAD);
        }
    }

    public static class DecreaseLoadRquestBuilder extends Builder {
        public DecreaseLoadRquestBuilder() {
            super(Request.HEADER_DECREASELOAD);
        }
    }

    public static class ListPhysicalNodesRquestBuilder extends Builder {
        public ListPhysicalNodesRquestBuilder() {
            super(Request.HEADER_LISTPHYSICALNODES);
        }
    }

    public static class PrintLookupTableRquestBuilder extends Builder {
        public PrintLookupTableRquestBuilder() {
            super(Request.HEADER_PRINTLOOKUPTABLE);
        }
    }

    public static class MoveBucketRquestBuilder extends Builder {
        public MoveBucketRquestBuilder() {
            super(Request.HEADER_MOVEBUCKET);
        }
    }

    public static class ExpandRquestBuilder extends Builder {
        public ExpandRquestBuilder() {
            super(Request.HEADER_EXPAND);
        }
    }

    public static class ShrinkRquestBuilder extends Builder {
        public ShrinkRquestBuilder() {
            super(Request.HEADER_SHRINK);
        }
    }
}
