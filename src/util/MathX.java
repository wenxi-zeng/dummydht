package util;

import java.util.*;

public class MathX {

    public static Queue<Integer> nonrepeatRandom(int bound, int size, List<Integer> exclude) {
        Random r = new Random();
        Set<Integer> s = new HashSet<>(exclude);
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

    public static Queue<Integer> nonrepeatRandom(int bound, int size) {
        return nonrepeatRandom(bound, size, new ArrayList<>());
    }

    public static int NextInt(int bound) {
        return (int)(Math.random() * (bound));
    }
}
