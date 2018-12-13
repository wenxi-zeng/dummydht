package util;

import socket.UDPClient;

import java.io.IOException;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SimpleLog {

    private static volatile SimpleLog instance = null;
    private static String identity;
    private UDPClient socketClient;
    private SimpleDateFormat dateFormatter;

    private SimpleLog() {
        try {
            dateFormatter = new SimpleDateFormat("MM-dd HH:mm:ss");
            socketClient = new UDPClient(Config.LOG_SERVER);
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public static void with(String address, int port) {
        SimpleLog.identity = address + ":" + port;
    }

    public static SimpleLog getInstance() {
        if (instance == null) {
            synchronized(SimpleLog.class) {
                if (instance == null) {
                    instance = new SimpleLog();
                }
            }
        }

        return instance;
    }

    public static synchronized void i(String message) {
        switch (Config.LOG_MODE) {
            case Config.LOG_MODE_SCREEN:
                v(message);
                break;
            case Config.LOG_MODE_SERVER:
                getInstance().send(message);
                break;
            case Config.LOG_MODE_FILE:
                break;
            case Config.LOG_MODE_OFF:
                break;
        }
    }

    public static synchronized void v(String message) {
        System.out.println(message);
    }

    public static synchronized void l(String message) {
        System.out.println(message);
    }

    public synchronized void send(String message) {
        if (socketClient != null) {
            try {
                socketClient.send(String.format("[%s %s]: %s", identity, dateFormatter.format(new Date()), message));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
