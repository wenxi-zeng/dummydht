package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import commonmodels.transport.JacksonObject;

import java.io.*;
import java.nio.ByteBuffer;

public class ObjectConverter {

    public static byte[] getBytes(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(o);
        out.flush();

        return bos.toByteArray();
    }

    public static Object getObject(byte[] bytes){
        try{
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInput in = new ObjectInputStream(bis);
            Object o = in.readObject();
            in.close();

            return o;
        } catch(Exception ex){
            ex.printStackTrace();
            return null;
        }
    }

    public static ByteBuffer getByteBuffer(Object o) throws IOException {
        return ByteBuffer.wrap(getBytes(o));
    }

    public static Object getObject(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return getObject(bytes);
    }

    public static byte[] getBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static ByteBuffer getByteBuffer(JacksonObject o) throws IOException {
        return ByteBuffer.wrap(getBytes(o));
    }

    public static byte[] getBytes(JacksonObject object) throws IOException {
        return new ObjectMapper().writeValueAsBytes(object);
    }

    public static JacksonObject getJacksonObject(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return getJacksonObject(bytes);
    }

    public static JacksonObject getJacksonObject(byte[] buf){
        try {
            return new ObjectMapper().readValue(buf, JacksonObject.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
