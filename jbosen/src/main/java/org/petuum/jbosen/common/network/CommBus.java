package org.petuum.jbosen.common.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CommBus {

    public static final int NONE = 0;
    public static final int INPROC = 1;
    public static final int INTERPROC = 2;

    private static final ThreadLocal<NettyThreadCommInfo>
            threadInfo = new ThreadLocal<>();
    public static ConcurrentHashMap<Integer, BlockingQueue<Msg>>
            nettyMsgQueues;
    // Note: this stores both incoming (coming in) and connected (connection
    // established by this thread) connection channels.
    // <selfId, <DestId, Channel>>
    public static Map<Integer, ConcurrentHashMap<Integer, Channel>> channelMap;
    private static int eStart;
    private static int eEnd;
    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;

    public static void init(int eStart, int eEnd) {
        CommBus.eStart = eStart;
        CommBus.eEnd = eEnd;
        nettyMsgQueues = new ConcurrentHashMap<>();
        channelMap = new ConcurrentHashMap<>();
        for (int i = eStart; i < eEnd; i++) {
            nettyMsgQueues.put(getNettyMsgQueueId(i), new LinkedBlockingQueue<Msg>());
            channelMap.put(i, new ConcurrentHashMap<Integer, Channel>());
        }
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
    }

    public static void shutdown() {
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }

    public static void registerThread(Config config) {
        threadInfo.set(new NettyThreadCommInfo());
        threadInfo.get().entityId = config.entityId;
        threadInfo.get().lType = config.lType;

        if ((config.lType & INTERPROC) != 0) {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new NettyCommBusInitializer());

            bootstrap.option(ChannelOption.SO_BACKLOG, 128);
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

            bootstrap.bind(Integer.parseInt(config.networkAddr.split(":")[1]))
                    .syncUninterruptibly().channel();
        }
    }

    public static void deregisterThread() {
        threadInfo.remove();
    }

    public static void connectTo(int entityId, Msg connectMsg) {
        connectMsg.setDest(entityId);
        connectMsg.setSender(threadInfo.get().entityId);
        try {
            nettyMsgQueues.get(getNettyMsgQueueId(entityId)).put(connectMsg);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void connectTo(int entityId, String networkAddr, Msg connectMsg) {
        assert (!isLocalEntity(entityId));
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NettyCommBusInitializer());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

        String ip = networkAddr.split(":")[0];
        int port = Integer.parseInt(networkAddr.split(":")[1]);
        Channel ch;
        while (true) {
            try {
                ch = bootstrap.connect(ip, port).sync().channel();
                break;
            } catch (Exception e) {
                // wait a bit and try to connect again
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                    System.exit(1);
                }
            }
        }
        assert ch != null;
        channelMap.get(threadInfo.get().entityId).putIfAbsent(entityId, ch);
        sendInterproc(entityId, connectMsg);
    }

    public static boolean send(int entityId, Msg data) {
        if (isLocalEntity(entityId)) {
            return sendInproc(entityId, data);
        } else {
            return sendInterproc(entityId, data);
        }
    }

    public static boolean sendInproc(int entityId, Msg msg) {
        msg.setDest(entityId);
        msg.setSender(threadInfo.get().entityId);
        try {
            nettyMsgQueues.get(getNettyMsgQueueId(entityId)).put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return true;
    }

    public static boolean sendInterproc(int entityId, Msg msg) {
        Channel ch = channelMap.get(threadInfo.get().entityId).get(entityId);
        if (!ch.isActive()) {
            return false;
        }

        msg.setDest(entityId);
        msg.setSender(threadInfo.get().entityId);

        return ch.writeAndFlush(msg).syncUninterruptibly().isSuccess();
    }

    public static Msg recv() {
        Msg msg = null;
        try {
            msg = nettyMsgQueues.get(
                    getNettyMsgQueueId(threadInfo.get().entityId)).take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return msg;
    }

    public static Msg recv(long timeoutMilli) {
        try {
            Msg msg = nettyMsgQueues.get(
                    getNettyMsgQueueId(threadInfo.get().entityId)).poll(
                    timeoutMilli, TimeUnit.MILLISECONDS);
            if (msg == null) {
                return null;
            }
            return msg;
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    public static Msg recvAsync() {
        Msg msg = null;
        msg = nettyMsgQueues.get(
                getNettyMsgQueueId(threadInfo.get().entityId)).poll();
        if (msg == null) {
            return null;
        }
        return msg;
    }

    public static boolean isLocalEntity(int entityId) {
        return (eStart <= entityId) && (entityId <= eEnd);
    }

    public static int getNettyMsgQueueId(int entityId) {
        return entityId - eStart;
    }

    public static class Config {
        public int entityId;
        public int lType;
        public String networkAddr;

        public String ip;
        public int port;

        public Config() {
            this.entityId = 0;
            this.lType = CommBus.NONE;
        }

        public Config(int entityId, int lType, String networkAddr) {
            this.entityId = entityId;
            this.lType = lType;
            this.networkAddr = networkAddr;
        }

    }

}
