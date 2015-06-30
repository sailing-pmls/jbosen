package org.petuum.jbosen.common.network;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.BlockingQueue;

@Sharable
class NettyCommBusInboundHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj)
            throws Exception {

        Msg msg = (Msg) obj;

        int dest = msg.getDest();
        int sender = msg.getSender();

        CommBus.channelMap.get(dest).putIfAbsent(sender, ctx.channel());

        BlockingQueue<Msg> queue = CommBus.nettyMsgQueues.get(CommBus
                .getNettyMsgQueueId(dest));

        queue.put(msg);

    }

}