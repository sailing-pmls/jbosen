package org.petuum.jbosen.common.network;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Msg {

    protected static final int INT_LENGTH = Integer.SIZE / Byte.SIZE;

    private int msgType;
    private int sender;
    private int dest;
    private List<ByteBuffer> payloads;

    public Msg(int msgType) {
        this.payloads = new ArrayList<>();
        this.msgType = msgType;
        this.sender = -1;
        this.dest = -1;
    }

    protected Msg(Msg msg) {
        this.payloads = msg.payloads;
        this.msgType = msg.msgType;
        this.sender = -1;
        this.dest = -1;
    }

    public int getMsgType() {
        return msgType;
    }

    public int getSender() {
        return sender;
    }

    void setSender(int sender) {
        this.sender = sender;
    }

    protected int getNumPayloads() {
        return payloads.size();
    }

    protected ByteBuffer getPayload(int index) {
        if (payloads.get(index) != null) {
            ByteBuffer buffer = payloads.get(index).duplicate();
            buffer.rewind();
            return buffer;
        } else {
            return null;
        }
    }

    protected void addPayload(ByteBuffer payload) {
        if (payload != null) {
            ByteBuffer buffer = payload.duplicate();
            buffer.rewind();
            payloads.add(buffer);
        } else {
            payloads.add(null);
        }
    }

    protected void setPayload(int index, ByteBuffer payload) {
        if (payload != null) {
            ByteBuffer buffer = payload.duplicate();
            buffer.rewind();
            payloads.set(index, buffer);
        } else {
            payloads.set(index, null);
        }
    }

    int getDest() {
        return dest;
    }

    void setDest(int dest) {
        this.dest = dest;
    }
}
