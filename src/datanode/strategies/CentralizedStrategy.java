package datanode.strategies;

import commands.CommonCommand;
import commonmodels.DataNode;
import commonmodels.transport.Request;
import commonmodels.transport.Response;
import loadmanagement.LoadInfo;
import socket.SocketClient;
import util.Config;
import util.SimpleLog;

public class CentralizedStrategy extends MembershipStrategy implements SocketClient.ServerCallBack {

    public CentralizedStrategy(DataNode dataNode) {
        super(dataNode);
    }

    @Override
    public Response getMembersStatus() {
        return dataNode.execute(dataNode.prepareListPhysicalNodesCommand());
    }

    @Override
    public void onLoadInfoReported(LoadInfo loadInfo) {
        Request request = new Request()
                .withHeader(CommonCommand.UPDATELOAD.name())
                .withLargeAttachment(loadInfo);
        socketClient.send(
                Config.getInstance().getSeeds().get(0),
                request,
                this
        );
    }

    @Override
    public void onResponse(Request request, Response response) {
    }

    @Override
    public void onFailure(Request request, String error) {
    }
}
