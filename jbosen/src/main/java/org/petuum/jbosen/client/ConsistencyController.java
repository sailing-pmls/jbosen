package org.petuum.jbosen.client;

import org.petuum.jbosen.common.ThreadContext;
import org.petuum.jbosen.row.RowUpdate;

class ConsistencyController {

    private ProcessStorage processStorage;
    private int tableId;
    private int staleness;
    private Oplog oplog;

    public ConsistencyController(int tableId, int staleness,
                                 ProcessStorage processStorage, Oplog oplog) {
        this.staleness = staleness;
        this.oplog = oplog;
        this.processStorage = processStorage;
        this.tableId = tableId;
    }

    public ClientRow get(int rowId) {
        int stalestClock = Math.max(0, ThreadContext.getClock() - this.staleness);

        if (ThreadContext.getCachedSystemClock() < stalestClock) {
            int systemClock = BgThreadGroup.getSystemClock();
            if (systemClock < stalestClock) {
                BgThreadGroup.waitSystemClock(stalestClock);
                systemClock = BgThreadGroup.getSystemClock();
            }
            assert (systemClock >= stalestClock);
            ThreadContext.setCachedSystemClock(systemClock);
        }
        assert (ThreadContext.getCachedSystemClock() >= stalestClock);

        ClientRow clientRow = this.processStorage.getRow(rowId);

        if (clientRow != null) {
            //assert(clientRow.getClock() >= stalestClock);
            return clientRow;
        }

        // Didn't find rowId that's fresh enough in this.processStorage.
        // Fetch from server.
        int numFetches = 0;
        do {
            BgThreadGroup.requestRow(this.tableId, rowId, stalestClock);
            clientRow = this.processStorage.getRow(rowId);
            ++numFetches;
            assert (numFetches <= 3); // to prevent infinite loop
        } while (clientRow == null);

        assert (clientRow.getClock() >= stalestClock);
        return clientRow;
    }

    public void batchInc(int rowId, RowUpdate rowUpdate) {
        this.oplog.lockRow(rowId);
        try {
            RowUpdate rowOplog = this.oplog.getRowUpdate(rowId);
            if (rowOplog == null) {
                rowOplog = this.oplog.createRowUpdate(rowId);
            }

            rowOplog.inc(rowUpdate);
        } finally {
            this.oplog.unlockRow(rowId);
        }
    }
}
