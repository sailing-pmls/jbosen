package org.petuum.jbosen.common.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.List;

class NettyCommBusDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                          List<Object> out) throws Exception {
        if (in.readableBytes() < 4 * Integer.SIZE / Byte.SIZE) {
            return;
        }
        int numPayloads = in.getInt(in.readerIndex() + 3 * Integer.SIZE / Byte.SIZE);
        int index = 4 * Integer.SIZE / Byte.SIZE;
        for (int i = 0; i < numPayloads; i++) {
            if (in.readableBytes() < index + Integer.SIZE / Byte.SIZE) {
                return;
            }
            int size = in.getInt(in.readerIndex() + index);
            index += Integer.SIZE / Byte.SIZE + size;
            if (in.readableBytes() < index) {
                return;
            }
        }
        int sender = in.readInt();
        int dest = in.readInt();
        int msgType = in.readInt();
        numPayloads = in.readInt();
        Msg msg = new Msg(msgType);
        msg.setSender(sender);
        msg.setDest(dest);
        for (int i = 0; i < numPayloads; i++) {
            int size = in.readInt();
            if (size > 0) {
                ByteBuffer buffer = ByteBuffer.allocate(size);
                in.readBytes(buffer);
                buffer.rewind();
                msg.addPayload(buffer);
            } else {
                msg.addPayload(null);
            }
        }
        out.add(msg);
    }

}
