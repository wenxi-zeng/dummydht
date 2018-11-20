package socket;

import commonmodels.DataNode;

import java.nio.channels.AsynchronousSocketChannel;

public class DataNodeClient {

    private AsynchronousSocketChannel channel;

    private DataNode dataNode;

    public DataNodeClient(DataNode dataNode) {
        this.dataNode = dataNode;
    }

}
