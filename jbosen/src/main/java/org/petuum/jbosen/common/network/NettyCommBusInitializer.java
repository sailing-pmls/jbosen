package org.petuum.jbosen.common.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

class NettyCommBusInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast(new NettyCommBusEncoder());
        pipeline.addLast(new NettyCommBusDecoder());

        pipeline.addLast(new NettyCommBusInboundHandler());
    }

}