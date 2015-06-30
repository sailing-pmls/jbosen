package org.petuum.jbosen.server;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowFactory;
import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

class ServerTable {

    private RowFactory rowFactory;
    private RowUpdateFactory rowUpdateFactory;
    private TIntObjectMap<ServerRow> storage;

    public ServerTable(RowFactory rowFactory, RowUpdateFactory rowUpdateFactory) {
        this.storage = new TIntObjectHashMap<>();
        this.rowFactory = rowFactory;
        this.rowUpdateFactory = rowUpdateFactory;
    }

    public void resetDirty() {
        TIntObjectIterator<ServerRow> iter = storage.iterator();
        while (iter.hasNext()) {
            iter.advance();
            ServerRow serverRow = iter.value();
            serverRow.resetDirty();
        }
    }

    public ServerRow findRow(int rowId) {
        return storage.get(rowId);
    }

    public ServerRow createRow(int rowId) {
        Row rowData = this.rowFactory.createRow();
        storage.put(rowId, new ServerRow(rowData));
        return storage.get(rowId);
    }

    public boolean applyRowOpLog(int rowId, RowUpdate updates) {
        ServerRow serverRow = storage.get(rowId);
        if (serverRow == null) {
            return false;
        }
        serverRow.applyBatchInc(updates);
        return true;
    }

    public TIntSet getRowIdsToPush(int clientId) {
        TIntSet rowIds = new TIntHashSet();
        TIntObjectIterator<ServerRow> iter = storage.iterator();
        while (iter.hasNext()) {
            iter.advance();
            int rowId = iter.key();
            ServerRow serverRow = iter.value();
            if (serverRow.isSubscribed(clientId) && serverRow.isDirty()) {
                rowIds.add(rowId);
            }
        }
        return rowIds;
    }

    public RowFactory getRowFactory() {
        return this.rowFactory;
    }

    public RowUpdateFactory getRowUpdateFactory() {
        return this.rowUpdateFactory;
    }

}