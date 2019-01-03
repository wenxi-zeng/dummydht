package commonmodels.transport;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class Response implements Serializable
{

    private short status;
    private String message;
    private Object attachment;
    private final static long serialVersionUID = 7313299026043073913L;

    public final static short STATUS_SUCCESS = 0;
    public final static short STATUS_FAILED = 1;

    /**
     * No args constructor for use in serialization
     *
     */
    public Response() {
    }

    /**
     *
     * @param message
     * @param status
     * @param attachment
     */
    public Response(short status, String message, Object attachment) {
        super();
        this.status = status;
        this.message = message;
        this.attachment = attachment;
    }

    public short getStatus() {
        return status;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public Response withStatus(short status) {
        this.status = status;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Response withMessage(String message) {
        this.message = message;
        return this;
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public Response withAttachment(Object attachment) {
        this.attachment = attachment;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("status", status).append("message", message).append("attachment", attachment).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(message).append(status).append(attachment).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Response) == false) {
            return false;
        }
        Response rhs = ((Response) other);
        return new EqualsBuilder().append(message, rhs.message).append(status, rhs.status).append(attachment, rhs.attachment).isEquals();
    }

}
