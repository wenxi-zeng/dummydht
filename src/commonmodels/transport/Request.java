package commonmodels.transport;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "header",
        "sender",
        "receiver",
        "forward_to",
        "attachment",
        "epoch"
})
public class Request extends JacksonObject implements Serializable
{

    @JsonProperty("header")
    private String header;
    @JsonProperty("sender")
    private String sender;
    @JsonProperty("receiver")
    private String receiver;
    @JsonProperty("forward_to")
    private String forwardTo;
    @JsonProperty("attachment")
    private String attachment;
    @JsonProperty("epoch")
    private long epoch;
    private final static long serialVersionUID = 2464827715445932636L;

    public final static String HEADER_INITIALIZE = "initialize";
    public final static String HEADER_DESTROY = "destroy";
    public final static String HEADER_READ = "read";
    public final static String HEADER_WRITE = "write";
    public final static String HEADER_ADDNODE = "addNode";
    public final static String HEADER_REMOVENODE = "removeNode";
    public final static String HEADER_INCREASELOAD = "increaseLoad";
    public final static String HEADER_DECREASELOAD = "decreaseLoad";
    public final static String HEADER_LISTPHYSICALNODES = "listPhysicalNodes";
    public final static String HEADER_PRINTLOOKUPTABLE = "printLookupTable";
    public final static String HEADER_MOVEBUCKET = "moveBucket";
    public final static String HEADER_EXPAND = "expand";
    public final static String HEADER_SHRINK = "shrink";

    /**
     * No args constructor for use in serialization
     *
     */
    public Request() {
    }

    /**
     *
     * @param sender
     * @param receiver
     * @param epoch
     * @param attachment
     * @param forwardTo
     * @param header
     */
    public Request(String header, String sender, String receiver, String forwardTo, String attachment, long epoch) {
        super();
        this.header = header;
        this.sender = sender;
        this.receiver = receiver;
        this.forwardTo = forwardTo;
        this.attachment = attachment;
        this.epoch = epoch;
    }

    @JsonProperty("header")
    public String getHeader() {
        return header;
    }

    @JsonProperty("header")
    public void setHeader(String header) {
        this.header = header;
    }

    public Request withHeader(String header) {
        this.header = header;
        return this;
    }

    @JsonProperty("sender")
    public String getSender() {
        return sender;
    }

    @JsonProperty("sender")
    public void setSender(String sender) {
        this.sender = sender;
    }

    public Request withSender(String sender) {
        this.sender = sender;
        return this;
    }

    @JsonProperty("receiver")
    public String getReceiver() {
        return receiver;
    }

    @JsonProperty("receiver")
    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public Request withReceiver(String receiver) {
        this.receiver = receiver;
        return this;
    }

    @JsonProperty("forward_to")
    public String getForwardTo() {
        return forwardTo;
    }

    @JsonProperty("forward_to")
    public void setForwardTo(String forwardTo) {
        this.forwardTo = forwardTo;
    }

    public Request withForwardTo(String forwardTo) {
        this.forwardTo = forwardTo;
        return this;
    }

    @JsonProperty("attachment")
    public String getAttachment() {
        return attachment;
    }

    @JsonProperty("attachment")
    public void setAttachment(String attachment) {
        this.attachment = attachment;
    }

    public Request withAttachment(String attachment) {
        this.attachment = attachment;
        return this;
    }

    @JsonProperty("epoch")
    public long getEpoch() {
        return epoch;
    }

    @JsonProperty("epoch")
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public Request withEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("header", header).append("sender", sender).append("receiver", receiver).append("forwardTo", forwardTo).append("attachment", attachment).append("epoch", epoch).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(sender).append(receiver).append(epoch).append(attachment).append(forwardTo).append(header).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Request) == false) {
            return false;
        }
        Request rhs = ((Request) other);
        return new EqualsBuilder().append(sender, rhs.sender).append(receiver, rhs.receiver).append(epoch, rhs.epoch).append(attachment, rhs.attachment).append(forwardTo, rhs.forwardTo).append(header, rhs.header).isEquals();
    }

}