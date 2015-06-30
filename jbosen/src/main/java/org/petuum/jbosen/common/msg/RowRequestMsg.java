package org.petuum.jbosen.common.msg;

import org.petuum.jbosen.common.network.Msg;

import java.nio.ByteBuffer;

public class RowRequestMsg extends Msg {

    private static final int TABLE_ID_OFFSET = 0;
    private static final int ROW_ID_OFFSET = TABLE_ID_OFFSET + INT_LENGTH;
    private static final int CLOCK_OFFSET = ROW_ID_OFFSET + INT_LENGTH;
    private static final int FORCED_REQUEST_OFFSET = CLOCK_OFFSET + INT_LENGTH;

    private static final int DATA_SIZE = FORCED_REQUEST_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public RowRequestMsg() {
        super(MsgType.ROW_REQUEST);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
    }

    private RowRequestMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static RowRequestMsg wrap(Msg msg) {
        return new RowRequestMsg(msg);
    }

    public int getTableId() {
        return data.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        data.putInt(TABLE_ID_OFFSET, id);
    }

    public int getRowId() {
        return data.getInt(ROW_ID_OFFSET);
    }

    public void setRowId(int id) {
        data.putInt(ROW_ID_OFFSET, id);
    }

    public int getClock() {
        return data.getInt(CLOCK_OFFSET);
    }

    public void setClock(int clock) {
        data.putInt(CLOCK_OFFSET, clock);
    }

    public boolean getForcedRequest() {
        return data.getInt(FORCED_REQUEST_OFFSET) != 0;
    }

    public void setForcedRequest(boolean forcedRequest) {
        data.putInt(FORCED_REQUEST_OFFSET, forcedRequest ? 1 : 0);
    }

}
