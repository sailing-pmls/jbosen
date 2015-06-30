package org.petuum.jbosen.client;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.jbosen.common.GlobalContext;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class SparseProcessStorage implements ProcessStorage {

    private ArrayList<TIntObjectMap<ClientRow>> rows;
    private ArrayList<ReadWriteLock> locks;

    public SparseProcessStorage() {
        rows = new ArrayList<>();
        locks = new ArrayList<>();
        for (int i = 0; i < GlobalContext.getLockPoolSize(); i++) {
            rows.add(new TIntObjectHashMap<ClientRow>());
            locks.add(new ReentrantReadWriteLock());
        }
    }

    @Override
    public ClientRow getRow(int rowId) {
        locks.get(rowId % rows.size()).readLock().lock();
        try {
            return rows.get(rowId % rows.size()).get(rowId);
        } finally {
            locks.get(rowId % rows.size()).readLock().unlock();
        }
    }

    @Override
    public void putRow(int rowId, ClientRow row) {
        locks.get(rowId % rows.size()).writeLock().lock();
        try {
            rows.get(rowId % rows.size()).put(rowId, row);
        } finally {
            locks.get(rowId % rows.size()).writeLock().unlock();
        }
    }
}
