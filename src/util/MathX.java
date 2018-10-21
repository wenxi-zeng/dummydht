package util;

import java.util.*;

public class MathX {

    public static Queue<Integer> nonrepeatRandom(int bound, int size) {
        Random r = new Random();
        Set<Integer> s = new HashSet<>();
        for(int i = 0; i < size; i++){
            while(true) {
                int num = r.nextInt(bound);
                if (s.contains(num)) {
                    s.add(num);
                    break;
                }
            }
        }

        Queue<Integer> queue = new LinkedList<>(s);
        return queue;
    }

}
