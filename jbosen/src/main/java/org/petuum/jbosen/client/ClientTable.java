package org.petuum.jbosen.client;

import org.petuum.jbosen.row.RowFactory;
import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTable {

    private static final Logger logger = LoggerFactory.getLogger(ClientTable.class);

    private Oplog oplog;
    private ProcessStorage processStorage;
    private ConsistencyController consistencyController;
    private RowFactory rowFactory;
    private RowUpdateFactory rowUpdateFactory;

    public ClientTable(int tableId, int staleness, RowFactory rowFactory, RowUpdateFactory rowUpdateFactory) {
        this.rowFactory = rowFactory;
        this.rowUpdateFactory = rowUpdateFactory;
        this.processStorage = new SparseProcessStorage();
        this.oplog = new Oplog(this.rowUpdateFactory);
        this.consistencyController = new ConsistencyController(tableId, staleness, this.processStorage, this.oplog);
    }

    public RowUpdateFactory getRowUpdateFactory() {
        return this.rowUpdateFactory;
    }

    public RowFactory getRowFactory() {
        return this.rowFactory;
    }

    public void inc(int rowId, RowUpdate rowUpdate) {
        consistencyController.batchInc(rowId, rowUpdate);
    }

    public ClientRow get(int rowId) {
        return consistencyController.get(rowId);
    }

    public ProcessStorage getProcessStorage() {
        return this.processStorage;
    }

    public Oplog getOplog() {
        return this.oplog;
    }

}
