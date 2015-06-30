package org.petuum.jbosen;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * This class specifies the configuration options for the global Bosen PS
 * instance. Internally, it uses {@link org.kohsuke.args4j} to parse command
 * line arguments. To add additional arguments used by the application, it is
 * possible to extend this class and annotate additional fields with
 * {@link org.kohsuke.args4j.Option}, and then using {@link #parse(String[])}
 * to parse an array of command-line arguments.
 */
public class PsConfig {
    /**
     * ID of this client, from 0 to numClients - 1.
     */
    @Option(name = "-clientId", required = true,
            usage = "ID of this client, from 0 to numClients - 1.")
    public int clientId = -1;

    /**
     * Path to the host file.
     */
    @Option(name = "-hostFile", required = true,
            usage = "Path to the host file.")
    public String hostFile = "";

    /**
     * Number of application worker threads run on every client. Default = 1.
     */
    @Option(name = "-numLocalWorkerThreads", required = false,
            usage = "Number of worker threads per client. Default = 1")
    public int numLocalWorkerThreads = 1;

    /**
     * Number of network threads run on every client. The recommended value is
     * one network thread per 4 to 8 worker threads. Default = 1.
     */
    @Option(name = "-numLocalCommChannels", required = false,
            usage = "Number of communication channels per client. The " +
                    "recommended value is one network thread per 4 to 8 " +
                    "worker threads. Default = 1.")
    public int numLocalCommChannels = 1;

    /**
     * Parse an array of command-line arguments and sets the fields of this
     * object accordingly. The array must not contain any unrecognized
     * arguments. New arguments can be added by extending this class and
     * using {@link org.kohsuke.args4j.Option} to annotate additional fields.
     *
     * @param args array of command-line arguments.
     */
    public void parse(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    PsConfig getCopy() {
        PsConfig config = new PsConfig();
        config.clientId = clientId;
        config.hostFile = hostFile;
        config.numLocalWorkerThreads = numLocalWorkerThreads;
        config.numLocalCommChannels = numLocalCommChannels;
        return config;
    }

}
