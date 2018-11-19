package elastic;

import commonmodels.DataNode;
import util.ResourcesLoader;

import java.util.ResourceBundle;

import static util.Config.CONFIG_ELASTIC;

public class ElasticDataNode extends DataNode {

    @Override
    public void initTerminal() {
        terminal = new ElasticTerminal();
        terminal.initialize();
    }

    @Override
    public ResourceBundle loadConfig() {
        return ResourcesLoader.getBundle(CONFIG_ELASTIC);
    }

}