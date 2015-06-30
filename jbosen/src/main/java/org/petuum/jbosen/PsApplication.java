package org.petuum.jbosen;

/**
 * This class abstracts away some of the boilerplate code common to application
 * initialization and execution. In particular, this class takes care of
 * {@code PsTableGroup} initialization and shutdown, as well as worker thread
 * spawning, registering, and de-registering. An application using this class
 * should extend from it and implement {@link #initialize()} and
 * {@link #runWorkerThread(int)}. The application can then be started by
 * calling {@link #run(PsConfig)}.
 */
public abstract class PsApplication {

    /**
     * This method is called after {@code PsTableGroup} is initialized and
     * before any worker threads are spawned. Initialization tasks such as
     * table creation and data loading should be done here. Initializing
     * {@code PsTableGroup} is not required.
     */
    public abstract void initialize();

    /**
     * This method executes the code for a single worker thread. The
     * appropriate number of threads are spawned and run this method.
     * Worker thread register and de-register are not required.
     *
     * @param threadId the local thread ID to run.
     */
    public abstract void runWorkerThread(int threadId);

    /**
     * This method runs the application. It first initializes PsTableGroup,
     * then calls {@link #initialize()}, and finally spawns
     * {@code numLocalWorkerThreads} threads, each of which runs a single
     * instance of {@link #runWorkerThread(int)}. This method returns after all
     * worker threads have completed and {@code PsTableGroup} has been shut
     * down.
     *
     * @param config the config object.
     */
    public void run(PsConfig config) {
        PsTableGroup.init(config);
        initialize();
        int numLocalWorkerThreads = PsTableGroup.getNumLocalWorkerThreads();
        Thread[] threads = new Thread[numLocalWorkerThreads];
        for (int i = 0; i < numLocalWorkerThreads; i++) {
            threads[i] = new PsWorkerThread();
            threads[i].start();
        }
        for (int i = 0; i < numLocalWorkerThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        PsTableGroup.shutdown();
    }

    private class PsWorkerThread extends Thread {
        @Override
        public void run() {
            int threadId = PsTableGroup.registerWorkerThread();
            PsApplication.this.runWorkerThread(threadId);
            PsTableGroup.deregisterWorkerThread();
        }
    }

}
