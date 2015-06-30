package org.petuum.jbosen.server;

class ServerRowRequest {
    public int bgId; // requesting bg thread id
    public int tableId;
    public int rowId;
    public int clock;

    public ServerRowRequest() {
        this.bgId = 0;
        this.tableId = 0;
        this.rowId = 0;
        this.clock = 0;
    }
}
