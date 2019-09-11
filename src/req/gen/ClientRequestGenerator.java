package req.gen;

import commands.RingCommand;
import commonmodels.Terminal;
import commonmodels.transport.InvalidRequestException;
import commonmodels.transport.Request;
import req.StaticTree;
import util.Config;

import java.util.HashMap;
import java.util.Map;

public class ClientRequestGenerator extends RequestGenerator {

    private final StaticTree tree;

    private final Terminal terminal; // we need terminal in order to tag the request with epoch

    public ClientRequestGenerator(StaticTree tree, Terminal terminal) {
        super(tree.getFileSize() - 1);
        this.tree = tree;
        this.terminal = terminal;
    }

    @Override
    public Request next() {
        Request header = headerGenerator.next();
        if (header.getHeader().equals(RingCommand.READ.name()))
            return nextRead();
        else
            return nextWrite();
    }

    public Request nextRead() {
        StaticTree.RandTreeNode file = tree.getFiles().get(generator.nextInt());
        String[] args = new String[] { RingCommand.READ.name(),  file.toString(), String.valueOf(file.getSize()) };
        Request request = null;
        try {
            request = terminal.translate(args);
            request.setEpoch(terminal.getEpoch());
        } catch (InvalidRequestException e) {
            e.printStackTrace();
        }

        return request;
    }

    public Request nextWrite() {
        StaticTree.RandTreeNode file = tree.getFiles().get(generator.nextInt());
        String[] args = new String[] { RingCommand.WRITE.name(),  file.toString(), String.valueOf(file.getSize()) };
        Request request = null;
        try {
            request = terminal.translate(args);
            request.setEpoch(terminal.getEpoch());
        } catch (InvalidRequestException e) {
            e.printStackTrace();
        }

        return request;
    }

    @Override
    public Map<Request, Double> loadRequestRatio() {
        double[] ratio = Config.getInstance().getReadWriteRatio();
        Map<Request, Double> map = new HashMap<>();
        map.put(new Request().withHeader(RingCommand.READ.name()), ratio[Config.RATIO_KEY_READ]);
        map.put(new Request().withHeader(RingCommand.WRITE.name()), ratio[Config.RATIO_KEY_WRITE]);
        return map;
    }
}
