package req;

import commonmodels.transport.Request;
import req.Rand.ExpGenerator;
import req.Rand.RandomGenerator;
import req.Rand.UniformGenerator;
import req.Rand.ZipfGenerator;
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
                loadGenerator(requestTypes.size()));
    }

    public abstract Request next() throws Exception;

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
