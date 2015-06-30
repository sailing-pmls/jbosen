package org.petuum.jbosen.common.msg;

public class MsgType {
    public static final int CLIENT_CONNECT = 0;
    public static final int SERVER_CONNECT = 1;
    public static final int ROW_REQUEST = 2;
    public static final int ROW_REQUEST_REPLY = 3;
    public static final int SERVER_ROW_REQUEST_REPLY = 4;
    public static final int BG_CLOCK = 5;
    public static final int BG_SEND_OP_LOG = 6;
    public static final int CLIENT_SEND_OP_LOG = 7;
    public static final int CONNECT_SERVER = 8;
    public static final int CLIENT_START = 9;
    public static final int WORKER_THREAD_REG = 10;
    public static final int WORKER_THREAD_REG_REPLY = 11;
    public static final int WORKER_THREAD_DEREG = 12;
    public static final int CLIENT_SHUT_DOWN = 13;
    public static final int SERVER_SHUTDOWN_ACK = 14;
    public static final int SERVER_PUSH_ROW = 15;
    public static final int GLOBAL_BARRIER = 16;
    public static final int GLOBAL_BARRIER_REPLY = 17;
}
