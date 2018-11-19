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

}
