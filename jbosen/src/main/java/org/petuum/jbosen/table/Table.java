package org.petuum.jbosen.table;

import org.petuum.jbosen.PsTableGroup;
import org.petuum.jbosen.client.ClientTable;
import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowUpdate;

/**
 * An object of this class offers a view of a Bosen PS SSP table. Methods of
 * this class should only be accessed from registered worker threads. This
 * class is thread-safe and can be shared between different worker threads.
 * Tables of this type can be retrieved using
 * {@link PsTableGroup#getTable(int)}.
 */
public final class Table {

    private ClientTable clientTable;

    /**
     * Internal use only.
     *
     * @param clientTable internal use only.
     */
    public Table(ClientTable clientTable) {
        this.clientTable = clientTable;
    }

    /**
     * Retrieve a row from this table. The row object itself is a cached
     * version stored in the current process. The row can be directly modified,
     * and these modifications will be seen from other worker threads in the
     * same process. However, these changes will never propagate to other
     * processes, and will be overwritten the next time the cache is updated.
     *
     * @param rowId     ID of the row to retrieve.
     * @param <RowType> type of the row to cast to before returning.
     * @return locally cached copy of the row.
     */
    @SuppressWarnings("unchecked")
    public <RowType extends Row> RowType get(int rowId) {
        return (RowType) clientTable.get(rowId).getRow();
    }

    /**
     * Apply an update to a certain row. This update is not seen immediately
     * at the local node, and will be seen before the staleness bound is
     * reached.
     *
     * @param rowId     ID of the row to update.
     * @param rowUpdate update object to apply.
     */
    public void inc(int rowId, RowUpdate rowUpdate) {
        clientTable.inc(rowId, rowUpdate);
    }

}
