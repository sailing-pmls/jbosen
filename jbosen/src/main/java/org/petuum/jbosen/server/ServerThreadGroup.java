package org.petuum.jbosen.server;

import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.row.RowFactory;
import org.petuum.jbosen.row.RowUpdateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ServerThreadGroup {

    private static final Logger logger = LoggerFactory.getLogger(ServerThreadGroup.class);

    private static ServerThread[] serverThreads;

    public static void init() {
        logger.debug("Initializing ServerThreadGroup.");
        serverThreads = new ServerThread[GlobalContext
                .getNumLocalCommChannels()];
        CyclicBarrier initBarrier = new CyclicBarrier(
                GlobalContext.getNumLocalCommChannels() + 1);
        for (int idx = 0; idx < GlobalContext.getNumLocalCommChannels(); idx++) {
            serverThreads[idx] = new ServerThread(
                    GlobalContext.getServerThreadId(
                            GlobalContext.getClientId(), idx),
                    initBarrier);
        }
        for (ServerThread serverThread : serverThreads) {
            serverThread.start();
        }
        try {
            initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
            System.exit(1);
        }
        logger.debug("ServerThreadGroup initialized.");
    }

    public static void shutdown() {
        logger.debug("Shutting down ServerThreadGroup.");
        for (ServerThread serverThread : serverThreads) {
            serverThread.shutdown();
        }
        logger.debug("ServerThreadGroup shut down.");
    }

    public static void createTable(int tableId, RowFactory rowFactory, RowUpdateFactory rowUpdateFactory) {
        for (ServerThread serverThread : serverThreads) {
            serverThread.createTable(tableId, rowFactory, rowUpdateFactory);
        }
    }
}
