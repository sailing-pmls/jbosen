package org.petuum.jbosen.common.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

class NettyThreadCommInfo {
    public int entityId;

    public BlockingQueue<Map.Entry<Channel, ByteBuffer>> inprocQueue;

    public int lType;
    public int pollSize;

    public int numBytesInprocSendBuff;
    public int numBytesInprocRecvBuff;
    public int numBytesInterprocSendBuff;
    public int numBytesInterprocRecvBuff;

    public ServerBootstrap bootstrap;
    public ChannelFuture closeFuture;

}
