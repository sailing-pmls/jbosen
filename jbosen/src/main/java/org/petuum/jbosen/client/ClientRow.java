package org.petuum.jbosen.client;

import org.petuum.jbosen.row.Row;

public class ClientRow {

    private Row row;
    private int clock;

    public ClientRow(int clock, Row row) {
        this.row = row;
        this.clock = clock;
    }

    public int getClock() {
        return this.clock;
    }

    public Row getRow() {
        return this.row;
    }
}
