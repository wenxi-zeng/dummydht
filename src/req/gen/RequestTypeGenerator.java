package req.gen;

import commonmodels.transport.Request;
import req.rand.RandomGenerator;

import java.util.*;

public class RequestTypeGenerator {
    private Request[] types;
    private int len;
    private RandomGenerator gen;
    private double[] steps;

    static class ValueComparator implements Comparator<Map.Entry<Request, Double>> {
        @Override
        public int compare(Map.Entry<Request, Double> o1, Map.Entry<Request, Double> o2) {
            double neg = o2.getValue() - o1.getValue();
            return neg > 0 ? 1 : neg < 0 ? -1 : 0;
        }
    }

    public RequestTypeGenerator(Map<Request, Double> ratio, RandomGenerator uniform) {
        gen = uniform;
        len = ratio.size();
        types = new Request[len];
        steps = new double[len];
        List<Map.Entry<Request, Double>> sorted = new ArrayList<>(ratio.entrySet());
        sorted.sort(new ValueComparator());
        double sum = 0.0;
        int i = 0;
        for (Map.Entry<Request, Double> entry : sorted) {
            types[i] = entry.getKey();
            steps[i++] = entry.getValue();
//			System.out.println(entry.getKey()+":"+entry.getValue());
            sum += entry.getValue();
        }
        if (len > 0) {
            steps[0] = steps[0] / sum;
            for (i = 1; i < len; ++i) {
                steps[i] = steps[i] / sum + steps[i - 1];
            }
        }
    }

    public Request next() {
        double rd = gen.nextDouble();
        for (int i = 0; i < len; ++i) {
            if (rd < steps[i]) return types[i];
        }
        return null;
    }
}
