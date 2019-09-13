package req.gen;

import commonmodels.Terminal;
import commonmodels.transport.Request;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class SequentialRequestGenerator extends RequestGenerator{

    private final Terminal terminal;

    private final Map<Integer, Queue<String>> dataPool;

    private final int numOfRequest;

    public SequentialRequestGenerator(int numOfThreads, int numOfRequest, String fileName, Terminal terminal) throws IOException {
        super(0);

        this.terminal = terminal;
        this.numOfRequest = numOfRequest;
        dataPool = new ConcurrentHashMap<>();
        for (int i = 0; i < numOfThreads; i++)
            dataPool.put(i, new LinkedList<>());

        FileReader fr = new FileReader(fileName);
        BufferedReader reader = new BufferedReader(fr);
        loadFile(reader);
    }

    @Override
    public Request nextFor(int threadId) throws Exception {
        Queue<String> queue = dataPool.get(threadId);
        if (queue != null && !queue.isEmpty()) {
            Request request = terminal.translate(queue.poll());
            request.setEpoch(terminal.getEpoch());
            return request;
        }
        else return null;
    }

    @Override
    public Map<Request, Double> loadRequestRatio() {
        return new HashMap<>();
    }

    private void loadFile(BufferedReader reader) throws IOException {
        String line;
        int i = 0;
        int curr = 0;
        Queue<String> queue = dataPool.get(curr);
        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (!line.isEmpty()) {
                queue.add(line);
                if (++i % numOfRequest == 0) {
                    queue = dataPool.get(++curr);
                    if (queue == null) break;
                }
            }
        }
        reader.close();
    }
}
