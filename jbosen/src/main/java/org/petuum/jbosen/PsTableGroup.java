package org.petuum.jbosen;

import com.google.common.base.Preconditions;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.petuum.jbosen.client.BgThreadGroup;
import org.petuum.jbosen.client.ClientTable;
import org.petuum.jbosen.common.GlobalContext;
import org.petuum.jbosen.common.ThreadContext;
import org.petuum.jbosen.common.network.CommBus;
import org.petuum.jbosen.common.util.HostInfo;
import org.petuum.jbosen.common.util.VectorClockMT;
import org.petuum.jbosen.row.RowFactory;
import org.petuum.jbosen.row.RowUpdateFactory;
import org.petuum.jbosen.row.double_.DenseDoubleRowFactory;
import org.petuum.jbosen.row.double_.DenseDoubleRowUpdateFactory;
import org.petuum.jbosen.row.double_.SparseDoubleRowFactory;
import org.petuum.jbosen.row.double_.SparseDoubleRowUpdateFactory;
import org.petuum.jbosen.row.float_.DenseFloatRowFactory;
import org.petuum.jbosen.row.float_.DenseFloatRowUpdateFactory;
import org.petuum.jbosen.row.float_.SparseFloatRowFactory;
import org.petuum.jbosen.row.float_.SparseFloatRowUpdateFactory;
import org.petuum.jbosen.row.int_.DenseIntRowFactory;
import org.petuum.jbosen.row.int_.DenseIntRowUpdateFactory;
import org.petuum.jbosen.row.int_.SparseIntRowFactory;
import org.petuum.jbosen.row.int_.SparseIntRowUpdateFactory;
import org.petuum.jbosen.row.long_.DenseLongRowFactory;
import org.petuum.jbosen.row.long_.DenseLongRowUpdateFactory;
import org.petuum.jbosen.row.long_.SparseLongRowFactory;
import org.petuum.jbosen.row.long_.SparseLongRowUpdateFactory;
import org.petuum.jbosen.server.NameNode;
import org.petuum.jbosen.server.ServerThreadGroup;
import org.petuum.jbosen.table.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is the main interface to the Bosen PS. All of it's methods are
 * static, so a single process only has access to a global instance. An
 * application using this interface is expected to perform the following:
 * <ol>
 * <li>Construct a {@link PsConfig} object with the appropriate
 * configuration parameters.</li>
 * <li>Call {@link #init(PsConfig)}.</li>
 * <li>Create the necessary tables, and perform other initialization tasks
 * such as data loading.</li>
 * <li>Spawn {@link PsConfig#numLocalWorkerThreads} threads, each must first
 * call {@link #registerWorkerThread()}, do some application defined work,
 * and end with calling {@link #deregisterWorkerThread()}.</li>
 * <li>Call {@link #shutdown()}.</li>
 * </ol>
 */
public final class PsTableGroup {

    private static boolean isInitialized = false;
    private static int maxTableStaleness;
    private static AtomicInteger numWorkerThreadsRegistered;
    private static AtomicInteger numWorkerThreadsDeregistered;
    private static TIntObjectMap<ClientTable> tables;
    private static TIntSet doubleTables;
    private static TIntSet floatTables;
    private static TIntSet intTables;
    private static TIntSet longTables;
    private static VectorClockMT vectorClock;
    private static ThreadLocal<Boolean> isRegistered;

    /**
     * Initialize the global Bosen PS environment. This method must be called
     * before any other PsTableGroup interface methods can be accessed. This
     * method must be called exactly once per client.
     *
     * @param config configuration options for the Bosen PS.
     */
    public static synchronized void
    init(PsConfig config) {
        Preconditions.checkState(!isInitialized,
                "PsTableGroup has already been initialized!");
        Preconditions.checkArgument(new File(config.hostFile).isFile(),
                "Cannot find host file: %s", config.hostFile);
        Preconditions.checkArgument(config.numLocalWorkerThreads > 0,
                "Invalid numLocalWorkerThreads: %d",
                config.numLocalWorkerThreads);
        Preconditions.checkArgument(config.numLocalCommChannels > 0,
                "Invalid numLocalCommChannels: %d",
                config.numLocalCommChannels);
        TIntObjectMap<HostInfo> hostMap = readHostInfos(config.hostFile);
        Preconditions.checkArgument(
                config.clientId >= 0 && config.clientId < hostMap.size(),
                "Invalid clientId for %d clients: %d",
                hostMap.size(), config.clientId);
        numWorkerThreadsRegistered = new AtomicInteger(0);
        numWorkerThreadsDeregistered = new AtomicInteger(0);
        maxTableStaleness = 0;
        tables = new TIntObjectHashMap<>();
        doubleTables = new TIntHashSet();
        floatTables = new TIntHashSet();
        intTables = new TIntHashSet();
        longTables = new TIntHashSet();
        vectorClock = new VectorClockMT();
        isRegistered = new ThreadLocal<>();
        int localIdMin = GlobalContext.getThreadIdMin(config.clientId);
        int localIdMax = GlobalContext.getThreadIdMax(config.clientId);
        GlobalContext.init(config.numLocalCommChannels, config.numLocalWorkerThreads, hostMap, config.clientId);
        CommBus.init(localIdMin, localIdMax);
        if (GlobalContext.amINameNodeClient()) {
            NameNode.init();
        }
        ServerThreadGroup.init();
        BgThreadGroup.init(tables);
        isInitialized = true;
    }

    /**
     * Shuts down the global Bosen PS environment. This method must be called
     * after all computations are complete in order to properly shut down all
     * background tasks. This method must be called exactly once per client,
     * after all worker threads have been de-registered.
     */
    public static synchronized void
    shutdown() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        int numDereg = numWorkerThreadsDeregistered.get();
        Preconditions.checkState(numDereg == getNumLocalWorkerThreads(),
                "Not all worker threads have been de-registered!");
        ServerThreadGroup.shutdown();
        if (GlobalContext.amINameNodeClient()) {
            NameNode.shutdown();
        }
        BgThreadGroup.shutdown();
        CommBus.shutdown();
        isInitialized = false;
    }

    /**
     * Returns the ID of this client. Can only be used after
     * {@link #init(PsConfig)} has been called.
     *
     * @return the client ID, from 0 to numClients - 1, inclusive.
     */
    public static int getClientId() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        return GlobalContext.getClientId();
    }

    /**
     * Returns the total number of Bosen PS clients. Can only be used after
     * {@link #init(PsConfig)} has been called.
     *
     * @return total number of clients.
     */
    public static int getNumClients() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        return GlobalContext.getNumClients();
    }

    /**
     * Returns the number of worker threads that run on each client. Can only
     * be used after {@link #init(PsConfig)} has been called.
     *
     * @return number of local worker threads.
     */
    public static int getNumLocalWorkerThreads() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        return GlobalContext.getNumLocalWorkerThreads();
    }

    /**
     * Returns the total number of worker threads across all clients. This
     * value should be equal to numClients * numLocalWorkerThreads. Can only be
     * used after {@link #init(PsConfig)} has been called.
     *
     * @return total number of worker threads.
     */
    public static int getNumTotalWorkerThreads() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        return GlobalContext.getNumTotalWorkerThreads();
    }

    /**
     * Creates a Bosen PS table. This method is meant for those who wish to
     * define their own custom rows and row updates. Otherwise, using the
     * create[Dense/Sparse][Double/Float/Int/Long]Table methods are
     * recommended. This method must be called by each client in order to
     * create the table. This method can only be called after PsTableGroup
     * is initialized and before any worker thread is registered.
     *
     * @param tableId          int ID of this table.
     * @param staleness        SSP staleness parameter.
     * @param rowFactory       object used to produce rows.
     * @param rowUpdateFactory object used to produce row updates.
     */
    public static synchronized void
    createTable(int tableId, int staleness, RowFactory rowFactory,
                RowUpdateFactory rowUpdateFactory) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(numWorkerThreadsRegistered.get() == 0,
                "Cannot create table after a worker thread is registered!");
        Preconditions.checkArgument(!tables.containsKey(tableId),
                "Already created table with ID=" + tableId + "!");
        Preconditions.checkArgument(staleness >= 0,
                "Cannot create table with negative staleness value!");
        if (staleness > maxTableStaleness) {
            maxTableStaleness = staleness;
        }
        ServerThreadGroup.createTable(tableId, rowFactory, rowUpdateFactory);
        tables.put(tableId, new ClientTable(tableId, staleness, rowFactory, rowUpdateFactory));
    }

    /**
     * Retrieves a table that was created before worker threads are registered.
     * This method can only be called from inside of registered worker threads.
     *
     * @param tableId ID of the table to retrieve.
     * @return object used to access the table.
     */
    public static Table getTable(int tableId) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        return new Table(tables.get(tableId));
    }

    /**
     * Creates a dense double table, which can later be retrieved using
     * {@link #getDoubleTable(int) getDoubleTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId     int ID of this table.
     * @param staleness   SSP staleness parameter.
     * @param rowCapacity capacity of the stored rows.
     */
    public static synchronized void
    createDenseDoubleTable(int tableId, int staleness, int rowCapacity) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkArgument(rowCapacity > 0,
                "Cannot create dense table with row capacity: %d",
                rowCapacity);
        RowFactory rowFactory = new DenseDoubleRowFactory(rowCapacity);
        RowUpdateFactory rowUpdateFactory = new DenseDoubleRowUpdateFactory(rowCapacity);
        doubleTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Creates a sparse double table, which can later be retrieved using
     * {@link #getDoubleTable(int) getDoubleTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId   int ID of this table.
     * @param staleness SSP staleness parameter.
     */
    public static synchronized void
    createSparseDoubleTable(int tableId, int staleness) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        RowFactory rowFactory = new SparseDoubleRowFactory();
        RowUpdateFactory rowUpdateFactory = new SparseDoubleRowUpdateFactory();
        doubleTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Retrieves a table that was created using
     * {@link #createDenseDoubleTable(int, int, int) createDenseDoubleTable} or
     * {@link #createSparseDoubleTable(int, int) createSparseDoubleTable}. This
     * method can only be called from inside of registered worker threads.
     *
     * @param tableId ID of the table to retrieve.
     * @return object used to access the table.
     */
    public static DoubleTable getDoubleTable(int tableId) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        Preconditions.checkState(doubleTables.contains(tableId),
                "Mismatched table type for table ID: " + tableId);
        return new DoubleTable(tables.get(tableId));
    }

    /**
     * Creates a dense float table, which can later be retrieved using
     * {@link #getFloatTable(int) getFloatTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId     int ID of this table.
     * @param staleness   SSP staleness parameter.
     * @param rowCapacity capacity of the stored rows.
     */
    public static synchronized void
    createDenseFloatTable(int tableId, int staleness, int rowCapacity) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkArgument(rowCapacity > 0,
                "Cannot create dense table with row capacity: %d",
                rowCapacity);
        RowFactory rowFactory = new DenseFloatRowFactory(rowCapacity);
        RowUpdateFactory rowUpdateFactory = new DenseFloatRowUpdateFactory(rowCapacity);
        floatTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Creates a sparse float table, which can later be retrieved using
     * {@link #getFloatTable(int) getFloatTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId   int ID of this table.
     * @param staleness SSP staleness parameter.
     */
    public static synchronized void
    createSparseFloatTable(int tableId, int staleness) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        RowFactory rowFactory = new SparseFloatRowFactory();
        RowUpdateFactory rowUpdateFactory = new SparseFloatRowUpdateFactory();
        floatTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);

    }

    /**
     * Retrieves a table that was created using
     * {@link #createDenseFloatTable(int, int, int) createDenseFloatTable} or
     * {@link #createSparseFloatTable(int, int) createSparseFloatTable}. This
     * method can only be called from inside of registered worker threads.
     *
     * @param tableId ID of the table to retrieve.
     * @return object used to access the table.
     */
    public static FloatTable getFloatTable(int tableId) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        Preconditions.checkState(floatTables.contains(tableId),
                "Mismatched table type for table ID: " + tableId);
        return new FloatTable(tables.get(tableId));
    }

    /**
     * Creates a dense int table, which can later be retrieved using
     * {@link #getIntTable(int) getIntTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId     int ID of this table.
     * @param staleness   SSP staleness parameter.
     * @param rowCapacity capacity of the stored rows.
     */
    public static synchronized void
    createDenseIntTable(int tableId, int staleness, int rowCapacity) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkArgument(rowCapacity > 0,
                "Cannot create dense table with row capacity: %d",
                rowCapacity);
        RowFactory rowFactory = new DenseIntRowFactory(rowCapacity);
        RowUpdateFactory rowUpdateFactory = new DenseIntRowUpdateFactory(rowCapacity);
        intTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Creates a sparse int table, which can later be retrieved using
     * {@link #getIntTable(int) getIntTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId   int ID of this table.
     * @param staleness SSP staleness parameter.
     */
    public static synchronized void
    createSparseIntTable(int tableId, int staleness) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        RowFactory rowFactory = new SparseIntRowFactory();
        RowUpdateFactory rowUpdateFactory = new SparseIntRowUpdateFactory();
        intTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Retrieves a table that was created using
     * {@link #createDenseIntTable(int, int, int) createDenseIntTable} or
     * {@link #createSparseIntTable(int, int) createSparseIntTable}. This
     * method can only be called from inside of registered worker threads.
     *
     * @param tableId ID of the table to retrieve.
     * @return object used to access the table.
     */
    public static IntTable getIntTable(int tableId) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        Preconditions.checkState(intTables.contains(tableId),
                "Mismatched table type for table ID: " + tableId);
        return new IntTable(tables.get(tableId));
    }

    /**
     * Creates a dense long table, which can later be retrieved using
     * {@link #getLongTable(int) getLongTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId     int ID of this table.
     * @param staleness   SSP staleness parameter.
     * @param rowCapacity capacity of the stored rows.
     */
    public static synchronized void
    createDenseLongTable(int tableId, int staleness, int rowCapacity) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkArgument(rowCapacity > 0,
                "Cannot create dense table with row capacity: %d",
                rowCapacity);
        RowFactory rowFactory = new DenseLongRowFactory(rowCapacity);
        RowUpdateFactory rowUpdateFactory = new DenseLongRowUpdateFactory(rowCapacity);
        longTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Creates a sparse long table, which can later be retrieved using
     * {@link #getLongTable(int) getLongTable}. This method must be called
     * by each client in order to create the table. This method can only be
     * called after PsTableGroup is initialized and before any worker thread is
     * registered.
     *
     * @param tableId   int ID of this table.
     * @param staleness SSP staleness parameter.
     */
    public static synchronized void
    createSparseLongTable(int tableId, int staleness) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        RowFactory rowFactory = new SparseLongRowFactory();
        RowUpdateFactory rowUpdateFactory = new SparseLongRowUpdateFactory();
        longTables.add(tableId);
        createTable(tableId, staleness, rowFactory, rowUpdateFactory);
    }

    /**
     * Retrieves a table that was created using
     * {@link #createDenseLongTable(int, int, int) createDenseLongTable} or
     * {@link #createSparseLongTable(int, int) createSparseLongTable}. This
     * method can only be called from inside of registered worker threads.
     *
     * @param tableId ID of the table to retrieve.
     * @return object used to access the table.
     */
    public static LongTable getLongTable(int tableId) {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        Preconditions.checkState(longTables.contains(tableId),
                "Mismatched table type for table ID: " + tableId);
        return new LongTable(tables.get(tableId));
    }

    /**
     * Register an application worker thread. This must be called by each
     * worker thread after table creation and before accessing any tables.
     *
     * @return the local thread ID of the registered thread, from 0 to
     * numLocalWorkerThreads - 1, inclusive.
     */
    public static int registerWorkerThread() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        int workerThreadIdOffset = numWorkerThreadsRegistered.getAndIncrement();
        int threadId = GlobalContext.getLocalIdMin()
                + GlobalContext.WORKER_THREAD_ID_OFFSET + workerThreadIdOffset;
        ThreadContext.registerThread(threadId);
        CommBus.Config config = new CommBus.Config(threadId, CommBus.NONE, "");
        CommBus.registerThread(config);
        vectorClock.addClock(threadId, 0);
        BgThreadGroup.registerWorkerThread();
        isRegistered.set(true);
        return workerThreadIdOffset;
    }

    /**
     * De-registers an application worker thread. Each worker thread is
     * expected to call this before exiting. The system can only be shut
     * down when all worker threads have de-registered themselves.
     */
    public static void deregisterWorkerThread() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Cannot de-register thread that has not been registered!");
        BgThreadGroup.deregisterWorkerThread();
        CommBus.deregisterThread();
        numWorkerThreadsDeregistered.getAndIncrement();
        isRegistered.remove();
    }

    /**
     * Advance the clock for the calling application thread. This method
     * can only be called while a worker thread is registered.
     */
    public static void clock() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        ThreadContext.clock();
        int clock = vectorClock.tick(ThreadContext.getId());
        if (clock != 0) {
            BgThreadGroup.clockAllTables();
        }
    }

    /**
     * Threads that calls this method must be at the same clock. This method
     * blocks until all worker threads on all clients have called it as well.
     * After this method returns, the worker thread is guaranteed to see the
     * most recent updates to each table. This method can only be called while
     * a worker thread is registered.
     */
    public static void globalBarrier() {
        Preconditions.checkState(isInitialized,
                "PsTableGroup has not been initialized!");
        Preconditions.checkState(isRegistered.get() != null,
                "Illegal access from non-registered thread!");
        for (int i = 0; i < maxTableStaleness + 1; i++) {
            clock();
        }
        BgThreadGroup.globalBarrier();
    }

    private static TIntObjectMap<HostInfo> readHostInfos(String hostfile) {
        TIntObjectMap<HostInfo> hostMap = new TIntObjectHashMap<>();
        try {
            int idx = 0;
            Scanner scanner = new Scanner(new FileReader(hostfile));
            while (scanner.hasNext()) {
                String token = scanner.next();
                String[] parts = token.trim().split(":");
                Preconditions.checkArgument(parts.length == 2,
                        "Malformed token in host file: %s", token);
                hostMap.put(idx, new HostInfo(idx, parts[0], parts[1]));
                idx++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return hostMap;
    }
}