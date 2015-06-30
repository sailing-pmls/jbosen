package org.petuum.jbosen.client;

import gnu.trove.map.TIntObjectMap;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.common.util.VectorClockMT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BgThreadGroup {

    private static final Logger logger = LoggerFactory.getLogger(BgThreadGroup.class);

    private static ArrayList<BgThread> bgThreads;

    private static Lock systemClockMtx;
    private static Condition systemClockCV;
    private static AtomicInteger systemClock;

    public static void init(TIntObjectMap<ClientTable> tables) {
        bgThreads = new ArrayList<>();
        for (int i = 0; i < GlobalContext.getNumLocalCommChannels(); i++) {
            bgThreads.add(null);
        }
        int bgThreadIdSt = GlobalContext.getHeadBgId(GlobalContext
                .getClientId());
        CyclicBarrier initBarrier = new CyclicBarrier(
                GlobalContext.getNumLocalCommChannels() + 1);
        systemClockMtx = new ReentrantLock();
        systemClock = new AtomicInteger(0);
        VectorClockMT bgServerClock = new VectorClockMT();
        systemClockCV = systemClockMtx.newCondition();
        for (int i = 0; i < GlobalContext.getNumLocalCommChannels(); i++) {
            bgServerClock.addClock(bgThreadIdSt + i, 0);
        }
        for (int i = 0; i < bgThreads.size(); i++) {
            bgThreads.set(i, new BgThread(bgThreadIdSt + i, i, tables,
                    initBarrier, systemClock, systemClockMtx, systemClockCV,
                    bgServerClock));
        }
        for (BgThread bgThread : bgThreads) {
            bgThread.start();
        }
        try {
            initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void shutdown() {
        for (BgThread bgThread : bgThreads) {
            bgThread.shutdown();
        }
    }

    public static void registerWorkerThread() {
        bgThreads.get(0).registerWorkerThread();
    }

    public static void deregisterWorkerThread() {
        for (BgThread bgThread : bgThreads) {
            bgThread.deregisterWorkerThread();
        }
    }

    public static boolean requestRow(int tableId, int rowId, int clock) {
        int bgIdx = GlobalContext.getPartitionCommChannelIndex(rowId);
        return bgThreads.get(bgIdx).requestRow(tableId, rowId, clock);
    }

    public static void clockAllTables() {
        for (BgThread bgThread : bgThreads) {
            bgThread.clockAllTables();
        }
    }

    public static int getSystemClock() {
        return systemClock.get();
    }

    public static void waitSystemClock(int myClock) {
        systemClockMtx.lock();
        try {
            while (systemClock.get() < myClock) {
                systemClockCV.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            systemClockMtx.unlock();
        }
    }

    public static void globalBarrier() {
        bgThreads.get(0).globalBarrier();
    }
}
