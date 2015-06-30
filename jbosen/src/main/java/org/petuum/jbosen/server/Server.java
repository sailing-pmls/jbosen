package org.petuum.jbosen.server;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.jbosen.common.msg.ClientSendOpLogMsg;
import org.petuum.jbosen.common.util.VectorClock;
import org.petuum.jbosen.row.RowFactory;
import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class Server {
    private VectorClock bgClock;
    private TIntObjectHashMap<ServerTable> tables;
    // mapping <clock, table id> to an array of read requests
    private TIntObjectMap<TIntObjectMap<List<ServerRowRequest>>> clockBgRowRequests;
    // latest oplog version that I have received from a bg thread
    private TIntIntMap bgVersionMap;

    public Server() {
        this.bgClock = new VectorClock();
        this.bgVersionMap = new TIntIntHashMap();
        this.tables = new TIntObjectHashMap<>();
        this.clockBgRowRequests = new TIntObjectHashMap<>();
    }

    public TIntObjectHashMap<ServerTable> getTables() {
        return tables;
    }

    public void init(int serverId, int[] bgIds) {
        for (int bgId : bgIds) {
            bgClock.addClock(bgId, 0);
            bgVersionMap.put(bgId, -1);
        }
    }

    public void createTable(int tableId, RowFactory rowFactory, RowUpdateFactory rowUpdateFactory) {
        tables.put(tableId, new ServerTable(rowFactory, rowUpdateFactory));
    }

    public ServerRow findCreateRow(int tableId, int rowId) {
        ServerTable serverTable = tables.get(tableId);
        ServerRow serverRow = serverTable.findRow(rowId);
        if (serverRow != null)
            return serverRow;
        serverRow = serverTable.createRow(rowId);
        return serverRow;
    }

    public boolean clockUntil(int bgId, int clock) {
        int newClock = bgClock.tickUntil(bgId, clock);
        return newClock != 0;
    }

    public void addRowRequest(int bgId, int tableId, int rowId, int clock) {
        ServerRowRequest serverRowRequest = new ServerRowRequest();
        serverRowRequest.bgId = bgId;
        serverRowRequest.tableId = tableId;
        serverRowRequest.rowId = rowId;
        serverRowRequest.clock = clock;

        if (clockBgRowRequests.get(clock) == null) {
            TIntObjectMap<List<ServerRowRequest>> newEntry =
                    new TIntObjectHashMap<>();
            clockBgRowRequests.put(clock, newEntry);
        }

        if (clockBgRowRequests.get(clock).get(bgId) == null) {
            List<ServerRowRequest> newEntry = new ArrayList<>();
            clockBgRowRequests.get(clock).put(bgId, newEntry);
        }
        clockBgRowRequests.get(clock).get(bgId).add(serverRowRequest);
    }

    public void getFulfilledRowRequests(ArrayList<ServerRowRequest> requests) {
        int clock = bgClock.getMinClock();
        requests.clear();

        TIntObjectMap<List<ServerRowRequest>> bgRowRequests = clockBgRowRequests
                .get(clock);
        if (bgRowRequests == null) {
            return;
        }
        TIntObjectIterator<List<ServerRowRequest>> iter =
                bgRowRequests.iterator();
        while (iter.hasNext()) {
            iter.advance();
            requests.addAll(iter.value());
        }
        clockBgRowRequests.remove(clock);
    }

    public void applyOpLogUpdateVersion(ClientSendOpLogMsg clientSendOpLogMsg,
                                        int bgThreadId, int version) {
        bgVersionMap.put(bgThreadId, version);
        ByteBuffer indexBuffer = clientSendOpLogMsg.getIndexBuffer();
        int rowUpdateIdx = 0;
        while (indexBuffer.hasRemaining()) {
            int tableId = indexBuffer.getInt();
            int numRows = indexBuffer.getInt();
            ServerTable serverTable = tables.get(tableId);
            RowUpdateFactory rowUpdateFactory = serverTable.getRowUpdateFactory();
            for (int i = 0; i < numRows; i++) {
                int rowId = indexBuffer.getInt();
                ByteBuffer rowUpdateBuffer = clientSendOpLogMsg.getRowUpdateBuffer(rowUpdateIdx);
                rowUpdateIdx++;
                RowUpdate rowUpdate = rowUpdateFactory.deserializeRowUpdate(rowUpdateBuffer);
                boolean found = serverTable.applyRowOpLog(rowId, rowUpdate);
                if (!found) {
                    serverTable.createRow(rowId);
                    serverTable.applyRowOpLog(rowId, rowUpdate);
                }
            }
        }
    }

    int getMinClock() {
        return bgClock.getMinClock();
    }

    int getBgVersion(int bgThreadId) {
        return bgVersionMap.get(bgThreadId);
    }
}
