package commonmodels;

import commonmodels.transport.Request;
import commonmodels.transport.Response;
import datanode.DataNodeServer;
import filemanagement.FileTransferManager;
import socket.SocketClient;
import socket.SocketServer;

public interface Daemon extends SocketServer.EventHandler, SocketClient.ServerCallBack, FileTransferManager.FileTransferRequestCallBack{
    void exec() throws Exception;
    void startDataNodeServer() throws Exception;
    void stopDataNodeServer() throws Exception;
    DataNodeServer getDataNodeServer();
    Response processCommonCommand(Request o);
    Response processDataNodeCommand(Request o);
    void send(String address, int port, Request request, SocketClient.ServerCallBack callBack);
    void send(String address, Request request, SocketClient.ServerCallBack callBack);
}
