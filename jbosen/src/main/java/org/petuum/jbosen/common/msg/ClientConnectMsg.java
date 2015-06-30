package org.petuum.jbosen.common.msg;

import org.petuum.jbosen.common.network.Msg;

import java.nio.ByteBuffer;

public class ClientConnectMsg extends Msg {

    private static final int CLIENT_ID_OFFSET = 0;

    private static final int DATA_SIZE = CLIENT_ID_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public ClientConnectMsg() {
        super(MsgType.CLIENT_CONNECT);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
    }

    private ClientConnectMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static ClientConnectMsg wrap(Msg msg) {
        return new ClientConnectMsg(msg);
    }

    public int getClientId() {
        return data.getInt(CLIENT_ID_OFFSET);
    }

    public void setClientId(int id) {
        data.putInt(CLIENT_ID_OFFSET, id);
    }
}