package statmanagement;

import com.sun.istack.internal.NotNull;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

import java.util.LinkedList;
import java.util.Queue;

public class StatInfoManager {

    private Queue<StatInfo> queue;

    private StatInfoReporter reporter;

    private static volatile StatInfoManager instance = null;

    private StatInfoManager() {
        queue = new LinkedList<StatInfo>() {
            @Override
            public boolean add(StatInfo info) {
                boolean result = super.add(info);
                reporter.report();

                return result;
            }
        };
        reporter = new StatInfoReporter(this);
    }

    public static StatInfoManager getInstance() {
        if (instance == null) {
            synchronized(StatInfoManager.class) {
                if (instance == null) {
                    instance = new StatInfoManager();
                }
            }
        }

        return instance;
    }

    public Queue<StatInfo> getQueue() {
        return queue;
    }

    public void statResponse(@NotNull Request request, @NotNull Response response, long respSize) {
        StatInfo responseStat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_RESPONSE)
                .withSize(respSize)
                .calcElapsed(response.getTimestamp());
        StatInfo roundTripStat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_ROUND_TRIP)
                .calcElapsed(request.getTimestamp());

        queue.add(responseStat);
        queue.add(roundTripStat);
    }

    public void statRoundTripFailure(@NotNull Request request) {
        StatInfo stat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_ROUND_TRIP_FAILURE)
                .calcElapsed(request.getTimestamp());
        queue.add(stat);
    }

    public void statRequest(@NotNull Request request, long receiveStamp, long reqSize) {
        StatInfo stat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_REQUEST)
                .withSize(reqSize)
                .calcElapsed(request.getTimestamp(), receiveStamp);
        queue.add(stat);
    }

    public void statExecution(@NotNull Request request, long receiveStamp) {
        StatInfo stat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_EXECUTION)
                .calcElapsed(receiveStamp);
        stat.setElapsed(stat.getElapsed() + (long)request.getProcessTime());
        queue.add(stat);
    }

    public long getStamp() {
        return System.currentTimeMillis();
    }
}
