package org.petuum.app.ssp_demo;

import java.io.IOException;

import org.petuum.jbosen.*;
import org.kohsuke.args4j.Option;
import org.petuum.jbosen.row.int_.IntRow;
import org.petuum.jbosen.table.IntTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A demo to show SSP guarantee. Row i stores the clocks for client i's threads.
// This app shows that the clock from other workers is bounded by SSP.
public class SSPDemo extends PsApplication {
    
    private static final Logger logger =
            LoggerFactory.getLogger(SSPDemo.class);

    private int numIterations;
    private int staleness;

    public SSPDemo(int numIterations, int staleness) {
        this.numIterations = numIterations;
        this.staleness = staleness;
    }

    @Override
    public void initialize() {
        int rowCapacity = PsTableGroup.getNumLocalWorkerThreads();
        PsTableGroup.createDenseIntTable(CLOCK_TABLE_ID, staleness, rowCapacity);
    }

    // Worker thread logic.
    @Override
    public void runWorkerThread(int threadId) {

        int clientId = PsTableGroup.getClientId();
        int numClients = PsTableGroup.getNumClients();
        int numThreads = PsTableGroup.getNumLocalWorkerThreads();

        // Get table by ID.
        IntTable clockTable = PsTableGroup.getIntTable(CLOCK_TABLE_ID);

        for (int iter = 0; iter < numIterations; iter++) {
            // Check that we are reading from other workers with SSP
            // bound. This is the least clock number we should see.
            double lowerBound = iter - staleness;
            for (int cliId = 0; cliId < numClients; cliId ++) {
                IntRow row = clockTable.get(cliId);
                for (int thrId = 0; thrId < numThreads; thrId ++) {
                    assert row.get(thrId) >= lowerBound;
                }
            }

            if (threadId == 0) {
                System.out.println("Worker thread " + threadId
                        + ": I'm reading pretty fresh stuff!");
            }

            // Increment the clock for this thread.
            clockTable.inc(clientId, threadId, 1);

            // Finally, finish this clock.
            PsTableGroup.clock();
        }

        // globalBarrier makes subsequent read all fresh.
        PsTableGroup.globalBarrier();

        // Read updates. After globalBarrier all updates should
        // be visible.
        for (int cliId = 0; cliId < numClients; cliId ++) {
            IntRow row = clockTable.get(cliId);
            for (int thrId = 0; thrId < numThreads; thrId ++) {
                assert row.get(thrId) == numIterations;
            }
        }

        if (threadId == 0) {
            System.out.println("All entries are fresh after globalBarrier()!");
        }
    }

    // Each table in PS has an id.
    private static final int CLOCK_TABLE_ID = 0;

    // Command line arguments.
    private static class CmdArgs extends PsConfig {
        // HelloSSP parameters:
        @Option(name = "-numIterations", required = false, usage = "Number of iterations. Default = 10")
        public int numIterations = 10;

        @Option(name = "-staleness", required = false, usage = "Staleness of parameter tables. Default = 0")
        public int staleness = 0;
    }

    public static void main(String[] args) throws IOException {
        logger.info("Beginning SSP Demo");
        
        CmdArgs cmd = new CmdArgs();
        cmd.parse(args);

        SSPDemo sspDemo = new SSPDemo(cmd.numIterations, cmd.staleness);
        sspDemo.run(cmd);

        System.out.println("SSPDemo finished on client " + cmd.clientId + ".");
    }
}
