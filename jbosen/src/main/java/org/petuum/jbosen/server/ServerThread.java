package org.petuum.jbosen.server;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.set.TIntSet;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.common.ThreadContext;
import org.petuum.jbosen.common.msg.*;
import org.petuum.jbosen.common.network.CommBus;
import org.petuum.jbosen.common.network.Msg;
import org.petuum.jbosen.common.util.HostInfo;
import org.petuum.jbosen.common.util.IntBox;
import org.petuum.jbosen.common.util.PtrBox;
import org.petuum.jbosen.row.RowFactory;
import org.petuum.jbosen.row.RowUpdateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

class ServerThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ServerThread.class);

    private int myId;
    private int[] bgWorkerIds;
    private Server server;
    private int numShutdownBgs;
    private CyclicBarrier initBarrier;

    public ServerThread(int myId, CyclicBarrier initBarrier) {
        this.myId = myId;
        this.bgWorkerIds = new int[GlobalContext.getNumClients()];
        this.numShutdownBgs = 0;
        this.initBarrier = initBarrier;
        this.server = new Server();
    }

    public void shutdown() {
        try {
            join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void serverPushRow() {
        for (int clientId = 0; clientId < GlobalContext.getNumClients(); clientId++) {
            TIntObjectIterator<ServerTable> iter = server.getTables().iterator();
            ServerPushRowMsg serverPushRowMsg = new ServerPushRowMsg();
            TIntList index = new TIntLinkedList();
            index.add(server.getTables().size());
            while (iter.hasNext()) {
                iter.advance();
                int tableId = iter.key();
                ServerTable serverTable = iter.value();
                TIntSet rowIds = serverTable.getRowIdsToPush(clientId);
                index.add(tableId);
                index.add(rowIds.size());
                TIntIterator it = rowIds.iterator();
                while (it.hasNext()) {
                    int rowId = it.next();
                    ServerRow serverRow = serverTable.findRow(rowId);
                    ByteBuffer buffer = serverRow.serialize();
                    serverPushRowMsg.addRowBuffer(buffer);
                    index.add(rowId);
                }
            }
            ByteBuffer indexBuffer = ByteBuffer.allocate(index.size() * (Integer.SIZE / Byte.SIZE));
            TIntIterator it = index.iterator();
            while (it.hasNext()) {
                indexBuffer.putInt(it.next());
            }
            serverPushRowMsg.setIndexBuffer(indexBuffer);
            int commChannelIdx = GlobalContext.getCommChannelIndexServer(myId);
            int bgId = GlobalContext.getBgThreadId(clientId, commChannelIdx);
            serverPushRowMsg.setVersion(server.getBgVersion(bgId));
            serverPushRowMsg.setClock(server.getMinClock());
            serverPushRowMsg.setIsClock(true);
            CommBus.send(bgId, serverPushRowMsg);
        }
        TIntObjectIterator<ServerTable> iter = server.getTables().iterator();
        while (iter.hasNext()) {
            iter.advance();
            ServerTable serverTable = iter.value();
            serverTable.resetDirty();
        }
    }

    public void rowSubscribe(ServerRow serverRow, int clientId) {
        serverRow.subscribe(clientId);
    }

    public void setUpCommBus() {
        CommBus.Config commConfig = new CommBus.Config();
        commConfig.entityId = myId;
        if (GlobalContext.getNumClients() > 0) {
            commConfig.lType = CommBus.INPROC | CommBus.INTERPROC;
            HostInfo hostInfo = GlobalContext.getServerInfo(myId);
            commConfig.networkAddr = hostInfo.ip + ":" + hostInfo.port;
        } else {
            commConfig.lType = CommBus.INPROC;
        }

        CommBus.registerThread(commConfig);
    }

    public void connectToNameNode() {
        int nameNodeId = GlobalContext.getNameNodeId();
        Msg serverConnectMsg = new Msg(MsgType.SERVER_CONNECT);

        if (CommBus.isLocalEntity(nameNodeId)) {
            CommBus.connectTo(nameNodeId, serverConnectMsg);
        } else {
            HostInfo nameNodeInfo = GlobalContext.getNameNodeInfo();
            String nameNodeAddr = nameNodeInfo.ip + ":"
                    + nameNodeInfo.port;
            CommBus.connectTo(nameNodeId, nameNodeAddr, serverConnectMsg);
        }
    }

    public int getConnection(PtrBox<Boolean> isClient, IntBox clientId) {
        Msg msg = CommBus.recv();

        if (msg.getMsgType() == MsgType.CLIENT_CONNECT) {
            ClientConnectMsg clientConnectMsg = ClientConnectMsg.wrap(msg);
            isClient.value = true;
            clientId.intValue = clientConnectMsg.getClientId();
        } else {
            isClient.value = false;
        }
        return msg.getSender();
    }

    public void sendToAllBgThreads(Msg msg) {
        for (int bgWorkerId : bgWorkerIds) {
            CommBus.send(bgWorkerId, msg);
        }
    }

    public void initServer() {
        connectToNameNode();

        int numBgs;
        for (numBgs = 0; numBgs < GlobalContext.getNumClients(); ++numBgs) {
            IntBox clientId = new IntBox(0);
            PtrBox<Boolean> isClient = new PtrBox<>();

            int bgId = getConnection(isClient, clientId);
            bgWorkerIds[numBgs] = bgId;
        }

        server.init(myId, bgWorkerIds);

        sendToAllBgThreads(new Msg(MsgType.CLIENT_START));
    }

    public boolean handleShutdownMsg() {
        // When num_shutdown_bgs reaches the total number of clients, the server
        // reply to each bg with a ShutDownReply message
        numShutdownBgs++;
        if (numShutdownBgs == GlobalContext.getNumClients()) {
            Msg shutdownAckMsg = new Msg(MsgType.SERVER_SHUTDOWN_ACK);

            for (int i = 0; i < GlobalContext.getNumClients(); i++) {
                int bg_id = bgWorkerIds[i];
                CommBus.send(bg_id, shutdownAckMsg);
            }
            return true;
        }
        return false;
    }

    public void createTable(int tableId, RowFactory rowFactory, RowUpdateFactory rowUpdateFactory) {
        server.createTable(tableId, rowFactory, rowUpdateFactory);
    }

    public void handleRowRequest(int senderId, RowRequestMsg rowRequestMsg) {
        int tableId = rowRequestMsg.getTableId();
        int rowId = rowRequestMsg.getRowId();
        int clock = rowRequestMsg.getClock();
        int serverClock = server.getMinClock();

        if (serverClock < clock) {
            // not fresh enough, wait
            server.addRowRequest(senderId, tableId, rowId, clock);
            return;
        }

        int version = server.getBgVersion(senderId);

        ServerRow serverRow = server.findCreateRow(tableId, rowId);

        rowSubscribe(serverRow, GlobalContext.threadIdToClientId(senderId));

        replyRowRequest(senderId, serverRow, tableId, rowId, serverClock,
                version);
    }

    public void replyRowRequest(int bgId, ServerRow serverRow, int tableId,
                                int rowId, int serverClock, int version) {
        ServerRowRequestReplyMsg serverRowRequestReplyMsg = new ServerRowRequestReplyMsg();
        serverRowRequestReplyMsg.setTableId(tableId);
        serverRowRequestReplyMsg.setRowId(rowId);
        serverRowRequestReplyMsg.setClock(serverClock);
        serverRowRequestReplyMsg.setVersion(version);

        ByteBuffer buf = serverRow.serialize();
        buf.rewind();
        serverRowRequestReplyMsg.setRowData(buf);

        CommBus.send(bgId, serverRowRequestReplyMsg);
    }

    public void handleOpLogMsg(int senderId,
                               ClientSendOpLogMsg clientSendOpLogMsg) {
        boolean isClock = clientSendOpLogMsg.getIsClock();
        int version = clientSendOpLogMsg.getVersion();
        int bgClock = clientSendOpLogMsg.getBgClock();
        server.applyOpLogUpdateVersion(clientSendOpLogMsg, senderId, version);

        boolean clockChanged = false;

        if (isClock) {
            clockChanged = server.clockUntil(senderId, bgClock);
            if (clockChanged) {
                ArrayList<ServerRowRequest> requests = new ArrayList<>();
                server.getFulfilledRowRequests(requests);
                for (ServerRowRequest request : requests) {
                    int tableId = request.tableId;
                    int rowId = request.rowId;
                    int bgId = request.bgId;
                    int bgVersion = server.getBgVersion(bgId);
                    ServerRow serverRow = server.findCreateRow(tableId,
                            rowId);
                    rowSubscribe(serverRow,
                            GlobalContext.threadIdToClientId(bgId));
                    int serverClock = server.getMinClock();

                    replyRowRequest(bgId, serverRow, tableId, rowId,
                            serverClock, bgVersion);
                }

            }
        }
        if (clockChanged) {
            serverPushRow();
        }
    }

    @Override
    public void run() {
        ThreadContext.registerThread(myId);

        setUpCommBus();

        try {
            initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            System.exit(1);
        }

        initServer();

        while (true) {
            Msg msg = CommBus.recv();
            int senderId = msg.getSender();
            int msgType = msg.getMsgType();
            switch (msgType) {
                case MsgType.CLIENT_SHUT_DOWN:
                    boolean shutdown = handleShutdownMsg();
                    if (shutdown) {
                        CommBus.deregisterThread();
                        return;
                    }
                    break;
                case MsgType.ROW_REQUEST:
                    RowRequestMsg rowRequestMsg = RowRequestMsg.wrap(msg);
                    handleRowRequest(senderId, rowRequestMsg);
                    break;
                case MsgType.CLIENT_SEND_OP_LOG:
                    ClientSendOpLogMsg clientSendOpLogMsg = ClientSendOpLogMsg.wrap(msg);
                    handleOpLogMsg(senderId, clientSendOpLogMsg);
                    break;
                default:
                    logger.error("Unknown message type: {}", msgType);
                    System.exit(1);
            }
        }
    }
}
