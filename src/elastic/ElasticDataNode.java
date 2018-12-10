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

    @Override
    public void onNodeUp(String cluster, String ip, int port) {
        String command = String.format(ElasticCommand.ADDNODE.getParameterizedString(), cluster, ip, port);
        terminal.execute(command.split("\\s+"));
    }

    @Override
    public void onNodeDown(String ip, int port) {
        String command = String.format(ElasticCommand.REMOVENODE.getParameterizedString(), ip, port);
        terminal.execute(command.split("\\s+"));
    }

}