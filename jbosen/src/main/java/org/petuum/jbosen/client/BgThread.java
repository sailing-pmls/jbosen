package org.petuum.jbosen.client;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.common.ThreadContext;
import org.petuum.jbosen.common.msg.*;
import org.petuum.jbosen.common.network.CommBus;
import org.petuum.jbosen.common.network.Msg;
import org.petuum.jbosen.common.util.HostInfo;
import org.petuum.jbosen.common.util.VectorClock;
import org.petuum.jbosen.common.util.VectorClockMT;
import org.petuum.jbosen.row.Row;
import org.petuum.jbosen.row.RowUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

class BgThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(BgThread.class);

    protected int myId;
    protected ArrayList<Integer> serverIds;
    protected CyclicBarrier initBarrier;
    protected int myCommChannelIdx;
    protected TIntObjectMap<ClientTable> tables;
    protected int version;
    protected RowRequestMgr rowRequestMgr;
    protected int clientClock;
    protected int clockHasPushed;
    // initialized at Creation time, used in CreateSendOpLogs()
    // For server x, table y, the size of serialized OpLog is ...
    protected TIntObjectMap<TIntIntMap> serverTableOplogSizeMap;
    // The OpLog msg to each server
    protected TIntObjectMap<ClientSendOpLogMsg> serverOplogMsgMap;
    protected TIntIntMap tableNumBytesByServer;
    protected TIntList globalBarrierWorkerIds;
    protected Lock systemClockMtx;
    protected Condition systemClockCV;
    protected AtomicInteger systemClock;
    protected VectorClockMT bgServerClock;
    protected VectorClock serverVectorClock;

    public BgThread(int id, int commChannelIdx,
                    TIntObjectMap<ClientTable> tables, CyclicBarrier initBarrier,
                    AtomicInteger systemClock, Lock systemClockMtx,
                    Condition systemClockCV, VectorClockMT bgServerClock) {
        this.myId = id;
        this.myCommChannelIdx = commChannelIdx;
        this.tables = tables;
        this.version = 0;
        this.clientClock = 0;
        this.clockHasPushed = -1;
        this.initBarrier = initBarrier;
        this.serverTableOplogSizeMap = new TIntObjectHashMap<>();
        this.serverOplogMsgMap = new TIntObjectHashMap<>();
        this.tableNumBytesByServer = new TIntIntHashMap();
        this.serverIds = GlobalContext.getServerThreadIDs(myCommChannelIdx);
        for (Integer serverId : serverIds) {
            this.serverTableOplogSizeMap.put(serverId, new TIntIntHashMap());
            this.serverOplogMsgMap.put(serverId, null);
            this.tableNumBytesByServer.put(serverId, 0);
        }
        this.globalBarrierWorkerIds = new TIntLinkedList();
        this.systemClockMtx = systemClockMtx;
        this.systemClockCV = systemClockCV;
        this.bgServerClock = bgServerClock;
        this.systemClock = systemClock;
        this.rowRequestMgr = new RowRequestMgr();

        this.serverVectorClock = new VectorClock();
        for (int serverId : serverIds) {
            serverVectorClock.addClock(serverId, 0);
        }
    }

    public boolean sendMsg(Msg msg) {
        return CommBus.sendInproc(myId, msg);
    }

    public void registerWorkerThread() {
        sendMsg(new Msg(MsgType.WORKER_THREAD_REG));
        Msg msg = CommBus.recv();
        int senderId = msg.getSender();
        int msgType = msg.getMsgType();
        assert (senderId == myId);
        assert (msgType == MsgType.WORKER_THREAD_REG_REPLY);
    }

    public void deregisterWorkerThread() {
        sendMsg(new Msg(MsgType.WORKER_THREAD_DEREG));
    }

    public void clockAllTables() {
        sendMsg(new Msg(MsgType.BG_CLOCK));
    }

    public boolean requestRow(int tableId, int rowId, int clock) {
        RowRequestMsg requestRowMsg = new RowRequestMsg();
        requestRowMsg.setTableId(tableId);
        requestRowMsg.setRowId(rowId);
        requestRowMsg.setClock(clock);
        requestRowMsg.setForcedRequest(false);

        sendMsg(requestRowMsg);

        // Wait for response
        Msg msg = CommBus.recv();
        assert (msg.getMsgType() == MsgType.ROW_REQUEST_REPLY);

        return true;
    }

    protected void initCommBus() {
        CommBus.Config commConfig = new CommBus.Config();
        commConfig.entityId = this.myId;
        commConfig.lType = CommBus.INPROC;
        CommBus.registerThread(commConfig);
    }

    protected void bgServerHandshake() {
        // connect to name node
        int nameNodeId = GlobalContext.getNameNodeId();
        this.connectToNameNodeOrServer(nameNodeId);

        // wait for ConnectServerMsg
        Msg msg = CommBus.recv();

        assert (msg.getSender() == nameNodeId);
        assert (msg.getMsgType() == MsgType.CONNECT_SERVER);

        // connect to servers
        for (int serverId : this.serverIds) {
            connectToNameNodeOrServer(serverId);
        }

        // get messages from servers for permission to start
        for (int numStartedServers = 0; numStartedServers < GlobalContext
                .getNumClients() + 1; ++numStartedServers) {
            msg = CommBus.recv();
            assert (msg.getMsgType() == MsgType.CLIENT_START);
        }
    }

    protected void connectToNameNodeOrServer(int serverId) {
        ClientConnectMsg clientConnectMsg = new ClientConnectMsg();
        clientConnectMsg.setClientId(GlobalContext.getClientId());

        if (CommBus.isLocalEntity(serverId)) {
            CommBus.connectTo(serverId, clientConnectMsg);
        } else {
            HostInfo serverInfo;
            if (serverId == GlobalContext.getNameNodeId()) {
                serverInfo = GlobalContext.getNameNodeInfo();
            } else {
                serverInfo = GlobalContext.getServerInfo(serverId);
            }
            String serverAddr = serverInfo.ip + ":" + serverInfo.port;
            CommBus.connectTo(serverId, serverAddr, clientConnectMsg);
        }
    }

    public void shutdown() {
        try {
            this.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected void checkForwardRowRequestToServer(int appThreadId,
                                                  RowRequestMsg rowRequestMsg) {
        int tableId = rowRequestMsg.getTableId();
        int rowId = rowRequestMsg.getRowId();
        boolean forced = rowRequestMsg.getForcedRequest();

        if (!forced) {
            // Check if the row exists in process cache
            ClientTable table = null;
            if (this.tables.containsKey(tableId)) {
                table = this.tables.get(tableId);
            }
            assert (table != null);
            // check if it is in process storage
            ProcessStorage tableStorage = table.getProcessStorage();
            ClientRow clientRow = tableStorage.getRow(rowId);
            if (clientRow != null) {
                CommBus.sendInproc(appThreadId, new Msg(MsgType.ROW_REQUEST_REPLY));
                return;
            }
        }

        RowRequestInfo rowRequest = new RowRequestInfo();
        rowRequest.appThreadId = appThreadId;
        rowRequest.clock = rowRequestMsg.getClock();

        // Version in request denotes the update version that the row on server
        // can see. Which should be 1 less than the current version number.
        rowRequest.version = this.version - 1;

        boolean shouldBeSent = this.rowRequestMgr.addRowRequest(
                rowRequest, tableId, rowId);

        if (shouldBeSent) {
            int serverId = GlobalContext.getPartitionServerId(rowId,
                    this.myCommChannelIdx);

            CommBus.send(serverId, rowRequestMsg);
        }
    }

    protected void handleServerRowRequestReply(int serverId,
                                               ServerRowRequestReplyMsg serverRowRequestReplyMsg) {
        int tableId = serverRowRequestReplyMsg.getTableId();
        int rowId = serverRowRequestReplyMsg.getRowId();
        int clock = serverRowRequestReplyMsg.getClock();

        ClientTable clientTable = this.tables.get(tableId);
        ByteBuffer data = serverRowRequestReplyMsg.getRowData();
        Row row = clientTable.getRowFactory().deserializeRow(data);
        ClientRow clientRow = new ClientRow(clock, row);
        clientTable.getProcessStorage().putRow(rowId, clientRow);

        ArrayList<Integer> appThreadIds = new ArrayList<>();
        int clockToRequest = rowRequestMgr.informReply(tableId, rowId,
                clock, this.version, appThreadIds);

        if (clockToRequest >= 0) {
            RowRequestMsg rowRequestMsg = new RowRequestMsg();
            rowRequestMsg.setTableId(tableId);
            rowRequestMsg.setRowId(rowId);
            rowRequestMsg.setClock(clockToRequest);

            int partitionServerId = GlobalContext.getPartitionServerId(rowId,
                    myCommChannelIdx);

            CommBus.send(partitionServerId, rowRequestMsg);
        }

        Msg rowRequestReplyMsg = new Msg(MsgType.ROW_REQUEST_REPLY);

        for (Integer appThreadId : appThreadIds) {
            CommBus.sendInproc(appThreadId, rowRequestReplyMsg);
        }
    }

    protected long handleClockMsg(boolean clockAdvanced) {
        this.clockHasPushed = this.clientClock;
        for (Integer serverId : this.serverIds) {
            ClientSendOpLogMsg clientSendOpLogMsg = new ClientSendOpLogMsg();
            TIntList index = new TIntLinkedList();
            for (TIntObjectIterator<ClientTable> tablePair = this.tables
                    .iterator(); tablePair.hasNext(); ) {
                tablePair.advance();
                int tableId = tablePair.key();
                ClientTable table = tablePair.value();
                Oplog oplog = table.getOplog();
                TIntSet rowIds = oplog.getRowIds(myCommChannelIdx, serverId);
                index.add(tableId);
                index.add(rowIds.size());

                TIntIterator iter = rowIds.iterator();
                while (iter.hasNext()) {
                    int rowId = iter.next();
                    index.add(rowId);
                    RowUpdate rowUpdate = null;
                    oplog.lockRow(rowId);
                    try {
                        rowUpdate = oplog.removeRowUpdate(rowId);
                    } finally {
                        oplog.unlockRow(rowId);
                    }
                    assert (rowUpdate != null);
                    clientSendOpLogMsg.addRowUpdateBuffer(rowUpdate.serialize());
                }
            }
            ByteBuffer indexBuffer = ByteBuffer.allocate(
                    index.size() * (Integer.SIZE / Byte.SIZE));
            TIntIterator iter = index.iterator();
            while (iter.hasNext()) {
                indexBuffer.putInt(iter.next());
            }

            clientSendOpLogMsg.setIndexBuffer(indexBuffer);
            clientSendOpLogMsg.setIsClock(clockAdvanced);
            clientSendOpLogMsg.setClientId(GlobalContext.getClientId());
            clientSendOpLogMsg.setVersion(this.version);
            clientSendOpLogMsg.setBgClock(this.clockHasPushed + 1);
            CommBus.send(serverId, clientSendOpLogMsg);
        }
        ++this.version;
        return 0;
    }

    private void handleGlobalBarrierReply() {
        TIntIterator iter = this.globalBarrierWorkerIds.iterator();
        while (iter.hasNext()) {
            CommBus.sendInproc(iter.next(), new Msg(MsgType.GLOBAL_BARRIER_REPLY));
        }
        this.globalBarrierWorkerIds.clear();
    }

    private void handleGlobalBarrier(int senderId) {
        this.globalBarrierWorkerIds.add(senderId);
        if (this.globalBarrierWorkerIds.size() == GlobalContext.getNumLocalWorkerThreads()) {
            CommBus.send(GlobalContext.getNameNodeId(), new Msg(MsgType.GLOBAL_BARRIER));
        }
    }

    protected void handleServerPushRow(int senderId,
                                       ServerPushRowMsg serverPushRowMsg) {
        boolean isClock = serverPushRowMsg.getIsClock();

        IntBuffer indexBuffer = serverPushRowMsg.getIndexBuffer().asIntBuffer();
        int idx = 0;
        int numTables = indexBuffer.get();
        for (int i = 0; i < numTables; i++) {
            int tableId = indexBuffer.get();
            ClientTable clientTable = tables.get(tableId);
            int numRows = indexBuffer.get();
            for (int j = 0; j < numRows; j++) {
                int rowId = indexBuffer.get();
                ByteBuffer buffer = serverPushRowMsg.getRowBuffer(idx);
                idx++;
                ClientRow clientRow = clientTable.getProcessStorage().getRow(rowId);
                if (clientRow != null) {
                    Row row = clientTable.getRowFactory().deserializeRow(buffer);
                    clientRow = new ClientRow(serverPushRowMsg.getClock(), row);
                    clientTable.getProcessStorage().putRow(rowId, clientRow);
                }
            }
        }

        if (isClock) {
            int serverClock = serverPushRowMsg.getClock();
            int newClock = serverVectorClock.tickUntil(senderId, serverClock);

            if (newClock != 0) {
                int newSystemClock = bgServerClock.tick(myId);

                if (newSystemClock != 0) {
                    systemClock.incrementAndGet();
                    systemClockMtx.lock();
                    systemClockCV.signalAll();
                    systemClockMtx.unlock();
                }
            }
        }

    }

    public void globalBarrier() {
        sendMsg(new Msg(MsgType.GLOBAL_BARRIER));
        Msg msg = CommBus.recv();
        assert (msg.getMsgType() == MsgType.GLOBAL_BARRIER_REPLY);
    }

    public void run() {
        ThreadContext.registerThread(this.myId);

        this.initCommBus();
        this.bgServerHandshake();

        try {
            this.initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            System.exit(1);
        }

        int numDeregisteredWorkerThreads = 0;
        int numShutdownAckedServers = 0;

        if (myCommChannelIdx == 0) {
            TIntSet registeredWorkerIds = new TIntHashSet();
            while (true) {
                Msg msg = CommBus.recv();
                int senderId = msg.getSender();
                int msgType = msg.getMsgType();
                assert (msgType == MsgType.WORKER_THREAD_REG);
                assert (!registeredWorkerIds.contains(senderId));
                registeredWorkerIds.add(senderId);
                if (registeredWorkerIds.size() == GlobalContext.getNumLocalWorkerThreads()) {
                    break;
                }
            }
            int nameNodeId = GlobalContext.getNameNodeId();
            CommBus.send(nameNodeId, new Msg(MsgType.WORKER_THREAD_REG));
            Msg msg = CommBus.recv();
            int senderId = msg.getSender();
            int msgType = msg.getMsgType();
            assert (msgType == MsgType.WORKER_THREAD_REG_REPLY);
            assert (senderId == nameNodeId);
            TIntIterator iter = registeredWorkerIds.iterator();
            while (iter.hasNext()) {
                int workerId = iter.next();
                CommBus.send(workerId, new Msg(MsgType.WORKER_THREAD_REG_REPLY));
            }
        }

        while (true) {
            Msg msg = CommBus.recv();
            int senderId = msg.getSender();
            int msgType = msg.getMsgType();

            switch (msgType) {
                case MsgType.WORKER_THREAD_DEREG:
                    ++numDeregisteredWorkerThreads;
                    if (numDeregisteredWorkerThreads == GlobalContext.getNumLocalWorkerThreads()) {
                        Msg clientShutdownMsg = new Msg(MsgType.CLIENT_SHUT_DOWN);
                        int nameNodeId = GlobalContext.getNameNodeId();
                        CommBus.send(nameNodeId, clientShutdownMsg);
                        for (Integer serverId : this.serverIds) {
                            CommBus.send(serverId, clientShutdownMsg);
                        }
                    }
                    break;
                case MsgType.SERVER_SHUTDOWN_ACK:
                    ++numShutdownAckedServers;
                    if (numShutdownAckedServers == GlobalContext.getNumClients() + 1) {
                        CommBus.deregisterThread();
                        return;
                    }
                    break;
                case MsgType.ROW_REQUEST:
                    RowRequestMsg rowRequestMsg = RowRequestMsg.wrap(msg);
                    this.checkForwardRowRequestToServer(senderId, rowRequestMsg);
                    break;
                case MsgType.SERVER_ROW_REQUEST_REPLY:
                    ServerRowRequestReplyMsg serverRowRequestReplyMsg = ServerRowRequestReplyMsg.wrap(msg);
                    handleServerRowRequestReply(senderId, serverRowRequestReplyMsg);
                    break;
                case MsgType.BG_CLOCK:
                    handleClockMsg(true);
                    ++this.clientClock;
                    break;
                case MsgType.BG_SEND_OP_LOG:
                    handleClockMsg(false);
                    break;
                case MsgType.SERVER_PUSH_ROW:
                    ServerPushRowMsg serverPushRowMsg = ServerPushRowMsg.wrap(msg);
                    handleServerPushRow(senderId, serverPushRowMsg);
                    break;
                case MsgType.GLOBAL_BARRIER:
                    handleGlobalBarrier(msg.getSender());
                    break;
                case MsgType.GLOBAL_BARRIER_REPLY:
                    handleGlobalBarrierReply();
                    break;
                default:
                    logger.error("Unknown message type: {}", msgType);
                    System.exit(1);
            }
        }
    }
}
