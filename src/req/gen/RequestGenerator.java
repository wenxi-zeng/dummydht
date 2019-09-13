package req.gen;

import commonmodels.transport.Request;
import req.rand.ExpGenerator;
import req.rand.RandomGenerator;
import req.rand.UniformGenerator;
import req.rand.ZipfGenerator;
import util.Config;

import java.util.Map;

public abstract class RequestGenerator {

    protected final RandomGenerator generator;

    protected RequestTypeGenerator headerGenerator;

    public RequestGenerator(int requestUpper) {
        this.generator = loadGenerator(requestUpper);
        Map<Request, Double> requestTypes = loadRequestRatio();
        this.headerGenerator = new RequestTypeGenerator(
                loadRequestRatio(),
                new UniformGenerator(requestTypes.size()));
    }

    public abstract Request nextFor(int threadId) throws Exception;

    public abstract Map<Request, Double> loadRequestRatio();

    private RandomGenerator loadGenerator(int upper) {
        UniformGenerator generator = new UniformGenerator(upper);

        if (Config.getInstance().getRequestDistribution().equals(Config.REQUEST_DISTRIBUTION_EXP))
            return new ExpGenerator(
                    Config.getInstance().getExpLamda(),
                    upper,
                    generator);
        else if (Config.getInstance().getRequestDistribution().equals(Config.REQUEST_DISTRIBUTION_ZIPF))
            return new ZipfGenerator(
                    Config.getInstance().getZipfAlpha(),
                    upper,
                    generator);
        else
            return generator;
    }
}
