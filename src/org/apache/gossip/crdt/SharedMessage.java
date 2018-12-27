package org.apache.gossip.crdt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;
import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "sender",
        "subject",
        "content"
})
public class SharedMessage implements Serializable, Comparable<SharedMessage>
{

    @JsonProperty("sender")
    private String sender;
    @JsonProperty("subject")
    private String subject;
    @JsonProperty("content")
    private String content;
    private final static long serialVersionUID = -7679985136746777659L;

    @JsonProperty("sender")
    public String getSender() {
        return sender;
    }

    @JsonProperty("sender")
    public void setSender(String sender) {
        this.sender = sender;
    }

    public SharedMessage withSender(String sender) {
        this.sender = sender;
        return this;
    }

    @JsonProperty("subject")
    public String getSubject() {
        return subject;
    }

    @JsonProperty("subject")
    public void setSubject(String subject) {
        this.subject = subject;
    }

    public SharedMessage withSubject(String subject) {
        this.subject = subject;
        return this;
    }

    @JsonProperty("content")
    public String getContent() {
        return content;
    }

    @JsonProperty("content")
    public void setContent(String content) {
        this.content = content;
    }

    public SharedMessage withContent(String content) {
        this.content = content;
        return this;
    }

    @Override
    public String toString()  {
        return subject ;
    }

    public String toStringX() {
        return "SharedMessage [sender=" + sender + ", subject=" + subject + ", content=" + content + "]" ;
    }

    @Override
    public int hashCode() {
        return subject.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof SharedMessage) == false) {
            return false;
        }
        SharedMessage rhs = ((SharedMessage) other);
        return hashCode() == rhs.hashCode();
    }

    @Override
    public int compareTo(SharedMessage o) {
        return subject.compareTo(o.subject);
    }

    static class SharedMessageSerializer extends JsonSerializer<SharedMessage>
    {
        public SharedMessageSerializer() {
        }

        @Override
        public void serialize(SharedMessage value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json = ow.writeValueAsString(value);
            jgen.writeFieldName(json);
        }
    }

    static class SharedMessageDeserializer extends KeyDeserializer {
        public SharedMessageDeserializer() {
        }

        @Override
        public SharedMessage deserializeKey(
                String key,
                DeserializationContext ctxt) throws IOException {
            return new ObjectMapper().readValue(key, SharedMessage.class);
        }
    }
}