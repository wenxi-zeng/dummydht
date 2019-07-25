package statmanagement;

import com.sun.istack.internal.NotNull;
import commonmodels.transport.Request;
import commonmodels.transport.Response;

public class StatInfoManager {

    private StatInfoReporter reporter;

    private static volatile StatInfoManager instance = null;

    private StatInfoManager() {
        reporter = new StatInfoReporter();
        reporter.start();
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

        reporter.report(responseStat);
        reporter.report(roundTripStat);
    }

    public void statRoundTripFailure(@NotNull Request request) {
        StatInfo stat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_ROUND_TRIP_FAILURE)
                .calcElapsed(request.getTimestamp());
        reporter.report(stat);
    }

    public void statRequest(@NotNull Request request, long receiveStamp, long reqSize) {
        StatInfo stat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_REQUEST)
                .withSize(reqSize)
                .calcElapsed(request.getTimestamp(), receiveStamp);
        reporter.report(stat);
    }

    public void statExecution(@NotNull Request request, long receiveStamp) {
        StatInfo stat = new StatInfo()
                .withHeader(request.getHeader())
                .withToken(request.getToken())
                .withType(StatInfo.TYPE_EXECUTION)
                .calcElapsed(receiveStamp);
        stat.setElapsed(stat.getElapsed() + (long)request.getProcessTime());
        reporter.report(stat);
    }

    public long getStamp() {
        return System.currentTimeMillis();
    }
}
