package ring;

import commonmodels.DataNode;
import util.ResourcesLoader;

import java.util.ResourceBundle;

import static util.Config.CONFIG_RING;

public class RingDataNode extends DataNode {

    @Override
    public void initTerminal() {
        terminal = new RingTerminal();
        terminal.initialize();
    }

    @Override
    public ResourceBundle loadConfig() {
        return ResourcesLoader.getBundle(CONFIG_RING);
    }

}