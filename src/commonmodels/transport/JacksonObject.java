package commonmodels.transport;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Request.class, name = "Request"),
        @JsonSubTypes.Type(value = Response.class, name = "Response")
})
public class JacksonObject {
}
