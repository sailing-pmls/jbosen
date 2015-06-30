package org.petuum.jbosen.common.msg;

import org.petuum.jbosen.common.network.Msg;

import java.nio.ByteBuffer;

public class ServerPushRowMsg extends Msg {

    private static final int CLOCK_OFFSET = 0;
    private static final int VERSION_OFFSET = CLOCK_OFFSET + INT_LENGTH;
    private static final int IS_CLOCK_OFFSET = VERSION_OFFSET + INT_LENGTH;
    private static final int DATA_SIZE = IS_CLOCK_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public ServerPushRowMsg() {
        super(MsgType.SERVER_PUSH_ROW);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
        addPayload(null);
    }

    private ServerPushRowMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static ServerPushRowMsg wrap(Msg msg) {
        return new ServerPushRowMsg(msg);
    }


    public int getClock() {
        return data.getInt(CLOCK_OFFSET);
    }

    public void setClock(int clock) {
        data.putInt(CLOCK_OFFSET, clock);
    }

    public int getVersion() {
        return data.getInt(VERSION_OFFSET);
    }

    public void setVersion(int version) {
        data.putInt(VERSION_OFFSET, version);
    }

    public boolean getIsClock() {
        return data.getInt(IS_CLOCK_OFFSET) == 1;
    }

    public void setIsClock(boolean isClock) {
        int intIsClock = isClock ? 1 : 0;
        data.putInt(IS_CLOCK_OFFSET, intIsClock);
    }

    public ByteBuffer getIndexBuffer() {
        ByteBuffer dup = getPayload(1).duplicate();
        dup.rewind();
        return dup;
    }

    public void setIndexBuffer(ByteBuffer buffer) {
        ByteBuffer dup = buffer.duplicate();
        dup.rewind();
        setPayload(1, dup);
    }

    public ByteBuffer getRowBuffer(int index) {
        ByteBuffer dup = getPayload(index + 2).duplicate();
        dup.rewind();
        return dup;
    }

    public void addRowBuffer(ByteBuffer buffer) {
        ByteBuffer dup = buffer.duplicate();
        dup.rewind();
        addPayload(dup);
    }
}
