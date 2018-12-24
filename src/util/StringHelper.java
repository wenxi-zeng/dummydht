package util;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StringHelper {

    public static String join(int[] array) {
        return IntStream.of(array)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
    }

}
