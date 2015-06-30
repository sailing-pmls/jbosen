package org.petuum.jbosen.common.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

class NettyCommBusEncoder extends MessageToByteEncoder<Msg> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Msg msg,
                          ByteBuf out) throws Exception {
        out.writeInt(msg.getSender());
        out.writeInt(msg.getDest());
        out.writeInt(msg.getMsgType());
        out.writeInt(msg.getNumPayloads());
        int numPayloads = msg.getNumPayloads();
        for (int i = 0; i < numPayloads; i++) {
            ByteBuffer buffer = msg.getPayload(i);
            if (buffer != null) {
                out.writeInt(buffer.capacity());
                out.writeBytes(buffer);
            } else {
                out.writeInt(0);
            }
        }
    }

}
