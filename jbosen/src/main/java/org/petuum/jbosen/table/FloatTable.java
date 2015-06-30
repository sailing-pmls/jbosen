package org.petuum.jbosen.table;

import org.petuum.jbosen.PsTableGroup;
import org.petuum.jbosen.client.ClientTable;
import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.float_.FloatRow;
import org.petuum.jbosen.row.float_.FloatRowUpdate;
import org.petuum.jbosen.row.float_.SparseFloatRowUpdate;

/**
 * An object of this class offers a view of a table that contains rows of
 * float values. Methods of this class should only be accessed from registered
 * worker threads. This class is thread-safe and can be shared between
 * different worker threads. Tables of this type should be created with
 * {@link PsTableGroup#createDenseFloatTable(int, int, int)} or
 * {@link PsTableGroup#createSparseFloatTable(int, int)}, and be retrieved
 * with {@link PsTableGroup#getFloatTable(int)}.
 */
public final class FloatTable {

    private ClientTable clientTable;

    /**
     * Internal use only.
     *
     * @param clientTable internal use only.
     */
    public FloatTable(ClientTable clientTable) {
        this.clientTable = clientTable;
    }

    /**
     * Retrieve a row from this table.
     *
     * @param rowId     ID of the row to retrieve.
     * @param <RowType> type of the row to cast to before returning.
     * @return locally cached copy of the row.
     * @see Table#get(int)
     */
    @SuppressWarnings("unchecked")
    public <RowType extends FloatRow> RowType get(int rowId) {
        return (RowType) clientTable.get(rowId).getRow();
    }

    /**
     * Retrieve a single column from a row in this table.
     *
     * @param rowId    row ID of the value to retrieve.
     * @param columnId column ID of the value to retrieve.
     * @return the value at the given row and column.
     */
    public float get(int rowId, int columnId) {
        FloatRow floatRow = (FloatRow) clientTable.get(rowId).getRow();
        return floatRow.get(columnId);
    }

    /**
     * Apply a batch update to an entire row.
     *
     * @param rowId     ID of the row to update.
     * @param rowUpdate update object to apply.
     * @see Table#inc(int, RowUpdate)
     */
    public void inc(int rowId, FloatRowUpdate rowUpdate) {
        clientTable.inc(rowId, rowUpdate);
    }

    /**
     * Add a single value to a given row and column.
     *
     * @param rowId    row ID of the value to increment.
     * @param columnId column ID of the value to increment.
     * @param update   value to increment by.
     */
    public void inc(int rowId, int columnId, float update) {
        SparseFloatRowUpdate rowUpdate = new SparseFloatRowUpdate();
        rowUpdate.set(columnId, update);
        clientTable.inc(rowId, rowUpdate);
    }

}
