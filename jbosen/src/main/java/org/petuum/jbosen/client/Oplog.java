package org.petuum.jbosen.client;

import com.google.common.util.concurrent.Striped;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

class Oplog {
    private Striped<Lock> locks;
    private ArrayList<TIntObjectHashMap<OplogPartition>> oplogPartitionMaps;

    public Oplog(RowUpdateFactory rowUpdateFactory) {
        this.locks = Striped.lock(GlobalContext.getLockPoolSize());
        this.oplogPartitionMaps = new ArrayList<>();
        for (int i = 0; i < GlobalContext.getNumLocalCommChannels(); i++) {
            this.oplogPartitionMaps.add(new TIntObjectHashMap<OplogPartition>());
            ArrayList<Integer> serverIds = GlobalContext.getServerThreadIDs(i);
            for (int serverId : serverIds) {
                this.oplogPartitionMaps.get(i).put(serverId,
                        new SparseOplogPartition(rowUpdateFactory));
            }
        }
    }

    public void lockRow(int rowId) {
        this.locks.get(rowId).lock();
    }

    public void unlockRow(int rowId) {
        this.locks.get(rowId).unlock();
    }

    public RowUpdate createRowUpdate(int rowId) {
        int idx = GlobalContext.getPartitionCommChannelIndex(rowId);
        int serverId = GlobalContext.getPartitionServerId(rowId, idx);
        return this.oplogPartitionMaps.get(idx).get(serverId).createRowUpdate(rowId);
    }

    public RowUpdate getRowUpdate(int rowId) {
        int idx = GlobalContext.getPartitionCommChannelIndex(rowId);
        int serverId = GlobalContext.getPartitionServerId(rowId, idx);
        return this.oplogPartitionMaps.get(idx).get(serverId).getRowUpdate(rowId);
    }

    public RowUpdate removeRowUpdate(int rowId) {
        int idx = GlobalContext.getPartitionCommChannelIndex(rowId);
        int serverId = GlobalContext.getPartitionServerId(rowId, idx);
        return this.oplogPartitionMaps.get(idx).get(serverId).removeRowUpdate(rowId);
    }

    public TIntSet getRowIds(int commChannelIdx, int serverId) {
        return this.oplogPartitionMaps.get(commChannelIdx).get(serverId).getRowIds();
    }
}
