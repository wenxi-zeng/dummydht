package statmanagement;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import commonmodels.Queueable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "start_time",
        "end_time",
        "header",
        "token",
        "elapsed",
        "type"
})
public class StatInfo implements Serializable, Queueable
{
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;
    @JsonProperty("header")
    private String header;
    @JsonProperty("token")
    private String token;
    @JsonProperty("elapsed")
    private long elapsed;
    @JsonProperty("type")
    private String type;
    private final static long serialVersionUID = -3973190766843360141L;

    public static final String TYPE_REQUEST = "request_communication";
    public static final String TYPE_RESPONSE = "response_communication";
    public static final String TYPE_EXECUTION = "execution_computation";
    public static final String TYPE_ROUND_TRIP = "round_trip";
    public static final String TYPE_ROUND_TRIP_FAILURE = "round_trip_failure";

    /**
     * No args constructor for use in serialization
     *
     */
    public StatInfo() {
    }

    /**
     *
     * @param startTime
     * @param endTime
     * @param elapsed
     * @param token
     * @param type
     * @param header
     */
    public StatInfo(long startTime, long endTime, String header, String token, long elapsed, String type) {
        super();
        this.startTime = startTime;
        this.endTime = endTime;
        this.header = header;
        this.token = token;
        this.elapsed = elapsed;
        this.type = type;
    }

    @JsonProperty("start_time")
    public long getStartTime() {
        return startTime;
    }

    @JsonProperty("start_time")
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public StatInfo withStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    @JsonProperty("end_time")
    public long getEndTime() {
        return endTime;
    }

    @JsonProperty("end_time")
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public StatInfo withEndTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    @JsonProperty("header")
    public String getHeader() {
        return header;
    }

    @JsonProperty("header")
    public void setHeader(String header) {
        this.header = header;
    }

    public StatInfo withHeader(String header) {
        this.header = header;
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

    public StatInfo withToken(String token) {
        this.token = token;
        return this;
    }

    @JsonProperty("elapsed")
    public long getElapsed() {
        return elapsed;
    }

    @JsonProperty("elapsed")
    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }

    public StatInfo withElapsed(long elapsed) {
        this.elapsed = elapsed;
        return this;
    }

    public StatInfo calcElapsed(long startTime) {
        this.startTime = startTime;
        this.endTime = System.currentTimeMillis();
        this.elapsed = this.endTime - this.startTime;
        return this;
    }

    public StatInfo calcElapsed(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.elapsed = this.endTime - this.startTime;
        return this;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    public StatInfo withType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("startTime", startTime).append("endTime", endTime).append("header", header).append("token", token).append("elapsed", elapsed).append("type", type).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(startTime).append(endTime).append(elapsed).append(token).append(type).append(header).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof StatInfo) == false) {
            return false;
        }
        StatInfo rhs = ((StatInfo) other);
        return new EqualsBuilder().append(startTime, rhs.startTime).append(endTime, rhs.endTime).append(elapsed, rhs.elapsed).append(token, rhs.token).append(type, rhs.type).append(header, rhs.header).isEquals();
    }

}