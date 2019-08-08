package commonmodels;

import commonmodels.transport.Request;

public interface TableChangedHandler {
    void onTableChanged(Request delta, Object newTable);
}
