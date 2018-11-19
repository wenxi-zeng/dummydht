package util;

import java.net.URI;
import java.net.URISyntaxException;

public class URIHelper {

    public static URI getGossipURI(String ip, int port) throws URISyntaxException {
        return new URI("udp://" + ip + ":" + port);
    }

    public static URI getGossipURI(String address) throws URISyntaxException {
        return new URI("udp://" + address);
    }

}
