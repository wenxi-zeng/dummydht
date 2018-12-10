package ceph;

import commonmodels.DataNode;
import util.ResourcesLoader;

import java.util.Arrays;
import java.util.ResourceBundle;

import static util.Config.*;

public class CephDataNode extends DataNode {

    @Override
    public void initTerminal() {
        terminal = new CephTerminal();
        terminal.initialize();
    }

    @Override
    public ResourceBundle loadConfig() {
        return ResourcesLoader.getBundle(CONFIG_CEPH);
    }

    @Override
    public void onNodeUp(String cluster, String ip, int port) {
        String command = String.format(CephCommand.ADDNODE.getParameterizedString(), cluster, ip, port);
        terminal.execute(command.split("\\s+"));
    }

    @Override
    public void onNodeDown(String ip, int port) {
        String command = String.format(CephCommand.REMOVENODE.getParameterizedString(), ip, port);
        terminal.execute(command.split("\\s+"));
    }

}
