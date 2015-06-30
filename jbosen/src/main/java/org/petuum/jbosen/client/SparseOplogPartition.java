package org.petuum.jbosen.client;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.row.RowUpdate;
import org.petuum.jbosen.row.RowUpdateFactory;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class SparseOplogPartition implements OplogPartition {

    private ArrayList<TIntObjectMap<RowUpdate>> rowUpdateMap;
    private ArrayList<ReadWriteLock> locks;
    private RowUpdateFactory rowUpdateFactory;

    public SparseOplogPartition(RowUpdateFactory rowUpdateFactory) {
        rowUpdateMap = new ArrayList<>();
        locks = new ArrayList<>();
        for (int i = 0; i < GlobalContext.getLockPoolSize(); i++) {
            rowUpdateMap.add(new TIntObjectHashMap<RowUpdate>());
            locks.add(new ReentrantReadWriteLock());
        }
        this.rowUpdateFactory = rowUpdateFactory;
    }

    @Override
    public RowUpdate createRowUpdate(int rowId) {
        RowUpdate rowOplog = rowUpdateFactory.createRowUpdate();
        locks.get(rowId % rowUpdateMap.size()).writeLock().lock();
        try {
            rowUpdateMap.get(rowId % rowUpdateMap.size()).put(rowId, rowOplog);
            return rowOplog;
        } finally {
            locks.get(rowId % rowUpdateMap.size()).writeLock().unlock();
        }
    }

    @Override
    public RowUpdate getRowUpdate(int rowId) {
        locks.get(rowId % rowUpdateMap.size()).readLock().lock();
        try {
            return rowUpdateMap.get(rowId % rowUpdateMap.size()).get(rowId);
        } finally {
            locks.get(rowId % rowUpdateMap.size()).readLock().unlock();
        }
    }

    @Override
    public RowUpdate removeRowUpdate(int rowId) {
        locks.get(rowId % rowUpdateMap.size()).writeLock().lock();
        try {
            return rowUpdateMap.get(rowId % rowUpdateMap.size()).remove(rowId);
        } finally {
            locks.get(rowId % rowUpdateMap.size()).writeLock().unlock();
        }
    }

    @Override
    public TIntSet getRowIds() {
        TIntSet rowIds = new TIntHashSet();
        for (int i = 0; i < rowUpdateMap.size(); i++) {
            locks.get(i).readLock().lock();
            try {
                rowIds.addAll(rowUpdateMap.get(i).keySet());
            } finally {
                locks.get(i).readLock().unlock();
            }
        }
        return rowIds;
    }
}
