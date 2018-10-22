package util;

public class SimpleLog {

    public static synchronized void i(String message) {
        System.out.println(message);
    }

}
