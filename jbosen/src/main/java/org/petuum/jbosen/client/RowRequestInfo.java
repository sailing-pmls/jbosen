package org.petuum.jbosen.client;

class RowRequestInfo {
    public int appThreadId;
    public int clock;
    public int version;
    public boolean sent;

    public RowRequestInfo() {
        appThreadId = 0;
        clock = 0;
        version = 0;
    }

    public RowRequestInfo(RowRequestInfo other) {
        this.appThreadId = other.appThreadId;
        this.clock = other.clock;
        this.version = other.version;
    }
}
