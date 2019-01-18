package commonmodels;

import java.util.List;

public interface ReadWriteCallBack {
    void onFileWritten(String file, List<PhysicalNode> replicas);
}
