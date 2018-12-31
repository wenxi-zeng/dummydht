package util;

import socket.UDPClient;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SimpleLog {
    private static volatile SimpleLog instance = null;
    private String identity;
    private UDPClient socketClient;
    private SimpleDateFormat dateFormatter;

    private SimpleLog() {
        dateFormatter = new SimpleDateFormat("MM-dd HH:mm:ss");
    }

    public static void with(String address, int port) {
        SimpleLog.getInstance().identity = address + ":" + port;
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
        switch (Config.getInstance().getLogMode()) {
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
        try {
            if (socketClient == null) {
                socketClient = new UDPClient(Config.getInstance().getLogServer());
            }

            socketClient.send(String.format("[%s %s]: %s", identity, dateFormatter.format(new Date()), message));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
