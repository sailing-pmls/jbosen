package org.petuum.jbosen.server;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.common.ThreadContext;
import org.petuum.jbosen.common.msg.ClientConnectMsg;
import org.petuum.jbosen.common.msg.MsgType;
import org.petuum.jbosen.common.network.CommBus;
import org.petuum.jbosen.common.network.Msg;
import org.petuum.jbosen.common.util.HostInfo;
import org.petuum.jbosen.common.util.PtrBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

class NameNodeThread extends Thread {
    private static final Logger logger =
            LoggerFactory.getLogger(NameNodeThread.class);

    private CyclicBarrier initBarrier;
    private int[] bgWorkerIds;
    private Server server;
    private int numShutdownBgs;
    private int numGlobalBarrier;

    public NameNodeThread(CyclicBarrier initBarrier) {
        this.initBarrier = initBarrier;
        this.bgWorkerIds = new int[GlobalContext.getNumClients()
                * GlobalContext.getNumLocalCommChannels()];
        this.numShutdownBgs = 0;
        this.server = new Server();
        this.numGlobalBarrier = 0;
    }

    public void shutdown() {

    }

    // communication function
    public int getConnection(PtrBox<Boolean> isClient,
                             PtrBox<Integer> clientId) {
        Msg msg = CommBus.recv();
        int senderId = msg.getSender();
        int msgType = msg.getMsgType();
        if (msgType == MsgType.CLIENT_CONNECT) {
            ClientConnectMsg clientConnectMsg = ClientConnectMsg.wrap(msg);
            isClient.value = true;
            clientId.value = clientConnectMsg.getClientId();
        } else {
            isClient.value = false;
        }
        return senderId;

    }

    public void sendToAllServers(Msg msg) {
        TIntIterator iter = GlobalContext.getAllServerIds().iterator();
        while (iter.hasNext()) {
            int serverId = iter.next();
            boolean isSent = CommBus.send(serverId, msg);
            assert (isSent);
        }
    }

    public void sendToAllBgThreads(Msg msg) {
        for (int bgId : this.bgWorkerIds) {
            boolean isSent = CommBus.send(bgId, msg);
            assert (isSent);
        }
    }

    public void setUpCommBus() {
        CommBus.Config commConfig = new CommBus.Config();
        commConfig.entityId = 0;
        if (GlobalContext.getNumClients() > 0) {
            commConfig.lType = CommBus.INPROC | CommBus.INTERPROC;
            HostInfo host_info = GlobalContext.getNameNodeInfo();
            commConfig.networkAddr = host_info.ip + ":" + host_info.port;
        } else {
            commConfig.lType = CommBus.INPROC;
        }

        CommBus.registerThread(commConfig);
    }

    public void initNameNode() {
        int numBgs = 0;
        int numServers = 0;
        int numExpectedConns = 2 * GlobalContext.getNumTotalCommChannels();

        for (int numConnections = 0; numConnections < numExpectedConns; ++numConnections) {
            PtrBox<Integer> clientId = new PtrBox<>();
            PtrBox<Boolean> isClient = new PtrBox<>();
            int senderId = getConnection(isClient, clientId);
            if (isClient.value) {
                bgWorkerIds[numBgs] = senderId;
                ++numBgs;
            } else {
                ++numServers;
            }
        }

        this.server.init(0, bgWorkerIds);

        sendToAllBgThreads(new Msg(MsgType.CONNECT_SERVER));

        sendToAllBgThreads(new Msg(MsgType.CLIENT_START));

    }

    public boolean handleShutdownMsg() {
        // When numShutdownBgs reaches the total number of bg threads, the
        // server
        // reply to each bg with a ShutDownReply message
        ++numShutdownBgs;
        if (numShutdownBgs == GlobalContext.getNumTotalCommChannels()) {
            Msg shutdownAckMsg = new Msg(MsgType.SERVER_SHUTDOWN_ACK);
            for (int i = 0; i < GlobalContext.getNumTotalCommChannels(); ++i) {
                int bg_id = bgWorkerIds[i];
                CommBus.send(bg_id, shutdownAckMsg);
            }
            return true;
        }
        return false;
    }

    public void run() {
        ThreadContext.registerThread(0);

        // set up thread-specific server context
        setUpCommBus();
        try {
            initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            System.exit(1);
        }
        initNameNode();

        TIntSet bgIds = new TIntHashSet();
        while (true) {
            Msg msg = CommBus.recv();
            int senderId = msg.getSender();
            int msgType = msg.getMsgType();
            assert (msgType == MsgType.WORKER_THREAD_REG);
            assert (!bgIds.contains(senderId));
            bgIds.add(senderId);
            if (bgIds.size() == GlobalContext.getNumClients()) {
                break;
            }
        }
        TIntIterator iter = bgIds.iterator();
        while (iter.hasNext()) {
            int bgId = iter.next();
            CommBus.send(bgId, new Msg(MsgType.WORKER_THREAD_REG_REPLY));
        }

        while (true) {
            Msg msg = CommBus.recv();
            int msgType = msg.getMsgType();
            switch (msgType) {
                case MsgType.CLIENT_SHUT_DOWN: {
                    boolean shutdown = handleShutdownMsg();
                    if (shutdown) {
                        CommBus.deregisterThread();
                        return;
                    }
                    break;
                }
                case MsgType.GLOBAL_BARRIER:
                    handleGlobalBarrier();
                    break;
                default:
                    logger.error("Unknown message type: {}", msgType);
                    System.exit(1);
            }
        }
    }

    private void handleGlobalBarrier() {
        this.numGlobalBarrier++;
        if (this.numGlobalBarrier == GlobalContext.getNumClients()) {
            int numClients = GlobalContext.getNumClients();
            for (int clientIdx = 0; clientIdx < numClients; ++clientIdx) {
                int headBgId = GlobalContext.getHeadBgId(clientIdx);
                CommBus.send(headBgId, new Msg(MsgType.GLOBAL_BARRIER_REPLY));
            }
            this.numGlobalBarrier = 0;
        }
    }

}
