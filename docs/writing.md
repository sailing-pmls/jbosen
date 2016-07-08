The main interface to JBösen is the `PsTableGroup` class, found in the package `org.petuum.jbosen`. In general, a JBösen application will do the following:

1. Initialize the global `PsTableGroup` instance.
2. Use `PsTableGroup` to create tables that are needed.
3. Spawn worker threads and complete computation.
4. De-initialize the global `PsTableGroup` instance.

For a simple example, please see the SSPDemo app.

## Initializing `PsTableGroup`

`PsTableGroup.init(PsConfig config)` must be called before any other `PsTableGroup` methods are invoked. The `PsConfig` class contains several public fields that the application must set:

* `clientId`: The ID of this client, from 0, inclusive, to the number of clients, exclusive. This value must be set.
* `hostFile`: The absolute path to the host file in the local file system. This value must be set.
* `numLocalWorkerThreads`: The number of threads for every client. This value defaults to 1.
* `numLocalCommChannels`: The number of communication channels per client. The recommended value is one per 4 to 8 worker threads. This value defaults to 1.

For example, an application may begin as follows:

    public static void main(String[] args) {
        PsConfig config = new PsConfig();
        config.clientId = 0;
        config.hostFile = "/home/username/cluster_machines";
        config.numLocalWorkerThreads = 16;
        config.numLocalCommChannels = 2;
        PsTableGroup.init(config);
        ...
    }

### Using command-line arguments

For convenience, it is also possible to automatically fill out a `PsConfig` object by parsing command line arguments:

    public static void main(String[] args) {
        PsConfig config = new PsConfig();
        config.parse(args);
        PsTableGroup.init(config);
        ...
    }

The arguments that are passed to the main method are `-clientId`, `-hostFile`, `-numLocalWorkerThreads`, and `-numLocalCommChannels`. `PsConfig` is unable to parse an argument list with unknown arguments. However, it is useful for applications to also pass in its own arguments that are unknown to JBösen. There are two solutions to this problem. The first is to simply remove all app-specific arguments from the argument list before parsing it. The second, preferred option is to extend the PsConfig class, using args4j annotations to specify app-specific arguments, and then parse the derived class. For example:

    public class AppConfig extends PsConfig {
        @Option(name = "-numIterations", required = true,
                usage = "Number of iterations")
        public int numIterations;

        @Option(name = "-stepSize", required = true,
                usage = "Step size, default = 0.001")
        public double stepSize = 0.001;
    }
    
    public static void main(String[] args) {
        AppConfig config = new AppConfig();
        config.parse(args);
        PsTableGroup.init(config);
        ...
    }

To do this, the application will need to import `org.kohsuke.args4j.Option`, which is included with the JBösen jar file.

## Creating tables

After `PsTableGroup` has been initialized, it is possible to create the distributed tables needed by the application. This is done using the `createTable` method, which takes several parameters:

* `int tableId`: The identifier for this table, which must be unique. This identifier is used later to access this table.
* `int staleness`: The SSP staleness parameter, which bounds the asynchronous operation of the table. Briefly, a worker thread that has called `PsTableGroup.clock()` `n` times will see all updates by workers that have called `PsTableGroup.clock()` up to `n - staleness` times.
* `RowFactory rowFactory`: A factory object that produces rows stored by this table. The implementation of the row is up to the application.
* `RowUpdateFactory rowUpdateFactory`: A factory object that produces row updates stored by this table. Essentially, JBösen tables store `Row` objects, which are updated by applying `RowUpdate` objects to them. The implementation of the row update is up to the application.

Since implementing these rows and row updates can be a lot of work, JBösen provides some common pre-implemented tables. For example, `createDenseDoubleTable` creates a table that contains rows and row updates that are essentially fixed-length arrays of `double` values, and applying an update means element-wise addition. For the full list of pre-implemented tables, see the interface or JavaDoc for `PsTableGroup`.

## Spawning worker threads

After all the necessary tables are created, the application can spawn its worker threads. JBösen expects the number of worker threads to be exactly the number set in `PsConfig.numLocalWorkerThreads`. To inform JBösen that a thread is a worker thread, it must call `PsTableGroup.registerWorkerThread()` before accessing any tables. After doing so, the worker can use `PsTableGroup.getTable(int tableId)` to retrieve the previously created tables, and begin to use them. When the worker has completed running, it must call `PsTableGroup.deregisterWorkerThread()` to inform JBösen. Each worker thread may look something like the following:

    class WorkerThread extends Thread {
        @Override
        public void run() {
            PsTableGroup.registerWorkerThread();
            Table table = PsTableGroup.getTable(0);
            ...
            PsTableGroup.deregisterWorkerThread();
        }
    }

## Shut down JBösen

When all worker threads have been de-registered, the last thing to do is to shut down JBösen. This is done simply by calling `PsTableGroup.shutdown()`.
