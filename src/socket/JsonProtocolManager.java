package socket;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import commonmodels.Transportable;

import java.io.IOException;

public class JsonProtocolManager {

    private final ObjectMapper objectMapper;

    public JsonProtocolManager() {
        objectMapper = new ObjectMapper();
        objectMapper.enableDefaultTyping();
        objectMapper.registerModule(new JsonModule());
        objectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, false);
    }

    public byte[] write(Transportable message) throws IOException {
        return objectMapper.writeValueAsBytes(message);
    }

    public Transportable read(byte[] buf) throws IOException {
        return objectMapper.readValue(buf, Transportable.class);
    }

}
