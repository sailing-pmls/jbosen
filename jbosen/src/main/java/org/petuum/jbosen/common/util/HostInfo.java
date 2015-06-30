package org.petuum.jbosen.common.util;

public class HostInfo {

    public int id;
    public String ip;
    public String port;

    public HostInfo(int id, String ip, String port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    public HostInfo(HostInfo other) {
        id = other.id;
        ip = other.ip;
        port = other.port;
    }

}
