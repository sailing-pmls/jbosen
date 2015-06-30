package org.petuum.jbosen.server;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class NameNode {

    private static NameNodeThread nameNodeThread;

    public static void init() {
        CyclicBarrier initBarrier = new CyclicBarrier(2);
        nameNodeThread = new NameNodeThread(initBarrier);
        nameNodeThread.start();
        try {
            initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    public static void shutdown() {
        nameNodeThread.shutdown();
    }

}
