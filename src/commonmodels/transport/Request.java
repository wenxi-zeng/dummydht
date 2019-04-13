package commonmodels.transport;

import java.io.Serializable;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "header",
        "sender",
        "receiver",
        "followup",
        "attachment",
        "epoch",
        "token",
        "timestamp"
})
public class Request implements Serializable
{

    @JsonProperty("header")
    private String header;
    @JsonProperty("sender")
    private String sender;
    @JsonProperty("receiver")
    private String receiver;
    @JsonProperty("followup")
    private String followup;
    @JsonProperty("attachment")
    private String attachment;
    @JsonProperty("epoch")
    private long epoch;
    @JsonProperty("token")
    private String token;
    @JsonProperty("timestamp")
    private long timestamp;

    private Object largeAttachment;

    private transient float processTime = .0f;

    private final static long serialVersionUID = -2162237185763801887L;

    /**
     * No args constructor for use in serialization
     *
     */
    public Request() {
        token = UUID.randomUUID().toString();
        timestamp = System.currentTimeMillis();
    }

    /**
     *
     * @param timestamp
     * @param sender
     * @param followup
     * @param receiver
     * @param token
     * @param epoch
     * @param attachment
     * @param header
     */
    public Request(String header, String sender, String receiver, String followup, String attachment, long epoch, String token, long timestamp) {
        super();
        this.header = header;
        this.sender = sender;
        this.receiver = receiver;
        this.followup = followup;
        this.attachment = attachment;
        this.epoch = epoch;
        this.token = token;
        this.timestamp = timestamp;
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

    @JsonProperty("followup")
    public String getFollowup() {
        return followup;
    }

    @JsonProperty("followup")
    public void setFollowup(String followup) {
        this.followup = followup;
    }

    public Request withFollowup(String followup) {
        this.followup = followup;
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

    @JsonProperty("token")
    public String getToken() {
        return token;
    }

    @JsonProperty("token")
    public void setToken(String token) {
        this.token = token;
    }

    public Request withToken(String token) {
        this.token = token;
        return this;
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Request withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Request withAttachments(Object... attachments) {
        StringBuilder result = new StringBuilder();
        for (Object str : attachments) {
            result.append(str).append(" ");
        }

        this.attachment = result.toString().trim();
        return this;
    }

    public Object getLargeAttachment() {
        return largeAttachment;
    }

    public void setLargeAttachment(Object largeAttachment) {
        this.largeAttachment = largeAttachment;
    }

    public Request withLargeAttachment(Object largeAttachment) {
        this.largeAttachment = largeAttachment;
        return this;
    }

    public float getProcessTime() {
        return processTime;
    }

    public void addProcessTime(float processTime) {
        this.processTime += processTime;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                        .append("        \n").append("header", header)
                        .append("        \n").append("token", token)
                        .append("        \n").append("timestamp", timestamp)
                        .append("        \n").append("sender", sender)
                        .append("        \n").append("receiver", receiver)
                        .append("        \n").append("followup", followup)
                        .append("        \n").append("attachment", attachment)
                        .append("        \n").append("epoch", epoch)
                        .append("        \n").toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(timestamp).append(sender).append(followup).append(receiver).append(token).append(epoch).append(attachment).append(header).toHashCode();
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
        return new EqualsBuilder().append(timestamp, rhs.timestamp).append(sender, rhs.sender).append(followup, rhs.followup).append(receiver, rhs.receiver).append(token, rhs.token).append(epoch, rhs.epoch).append(attachment, rhs.attachment).append(header, rhs.header).isEquals();
    }

    public String toCommand() {
        String command = "";

        if (header != null)
            command += header + " ";

        if (receiver != null)
            command += receiver + " ";

        if (attachment != null)
            command +=  attachment + " ";

        return command.trim();
    }
}