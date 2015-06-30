package org.petuum.jbosen.server;

import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.BitSet;

class ServerRow {

    private BitSet subscriptions;
    private Row rowData;
    private int numClientsSubscribed;
    private boolean dirty;

    public ServerRow() {
        this.rowData = null;
        this.numClientsSubscribed = 0;
        this.dirty = false;
        this.subscriptions = new BitSet(GlobalContext.getNumClients());
    }

    public ServerRow(Row rowData) {
        this.rowData = rowData;
        this.numClientsSubscribed = 0;
        this.dirty = false;
        this.subscriptions = new BitSet(GlobalContext.getNumClients());
    }

    public ServerRow(ServerRow other) {
        this.rowData = other.rowData;
        this.numClientsSubscribed = other.numClientsSubscribed;
        this.dirty = other.dirty;
        this.subscriptions = new BitSet(GlobalContext.getNumClients());
    }

    public Row getRow() {
        return rowData;
    }

    public void applyBatchInc(RowUpdate rowUpdate) {
        rowData.inc(rowUpdate);
        dirty = true;
    }

    public ByteBuffer serialize() {
        return rowData.serialize();
    }

    public void subscribe(int clientId) {
        if (!subscriptions.get(clientId)) {
            numClientsSubscribed++;
        }
        subscriptions.set(clientId);
    }

    public boolean isSubscribed(int clientId) {
        return subscriptions.get(clientId);
    }

    public boolean noClientSubscribed() {
        return (numClientsSubscribed == 0);
    }

    public void unsubscribe(int clientId) {
        if (subscriptions.get(clientId)) {
            numClientsSubscribed--;
        }
        subscriptions.clear(clientId);
    }

    public boolean isDirty() {
        return dirty;
    }

    public void resetDirty() {
        dirty = false;
    }

}
