package statmanagement;

import commonmodels.Queueable;
import commonmodels.Transportable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class StatInfo extends Transportable implements Serializable, Queueable
{
    private long startTime;
    private long endTime;
    private String header;
    private String token;
    private long elapsed;
    private String type;
    private long size;
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

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public StatInfo withStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public StatInfo withEndTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public StatInfo withHeader(String header) {
        this.header = header;
        return this;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public StatInfo withToken(String token) {
        this.token = token;
        return this;
    }

    public long getElapsed() {
        return elapsed;
    }

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public StatInfo withType(String type) {
        this.type = type;
        return this;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public StatInfo withSize(long size) {
        this.size = size;
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