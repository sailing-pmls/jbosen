package org.petuum.app.matrixfact;

import java.util.ArrayList;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.petuum.jbosen.PsApplication;
import org.petuum.jbosen.PsTableGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixFact extends PsApplication {
	private static final Logger logger = LoggerFactory
			.getLogger(MatrixFact.class);

	private static final int LTableId = 0;
	private static final int RTableId = 1;

	private MatrixFactConfig config;

	private MatrixFactWorker.Config mfConfig;

	public MatrixFact(MatrixFactConfig config) {
		this.config = config;
		this.mfConfig = new MatrixFactWorker.Config();
		mfConfig.K = config.K;
		mfConfig.lambda = config.lambda;
		mfConfig.learningRateDecay = config.learningRateDecay;
		mfConfig.learningRateEta0 = config.learningRateEta0;
		mfConfig.numMiniBatchesPerEpoch = config.numMiniBatchesPerEpoch;
		mfConfig.staleness = config.staleness;
		mfConfig.outputPrefix = config.outputPrefix;
		mfConfig.numEpochs = config.numEpochs;
		mfConfig.LTableId = LTableId;
		mfConfig.RTableId = RTableId;

	}

	@Override
	public void initialize() {
		long loadTimebegin = System.currentTimeMillis();
		ArrayList<Rating> ratings = new ArrayList<Rating>();
		mfConfig.ratings = ratings;
		int[] dim = DataLoader.ReadData(config.dataFile, ratings);
		long loadTimeElapsed = System.currentTimeMillis() - loadTimebegin;
		logger.info("Client " + config.clientId + " read data (" + dim[0]
				+ " rows, " + dim[1] + " cols, " + ratings.size()
				+ " ratings) in " + loadTimeElapsed + "ms");
		mfConfig.numRows = dim[0];
		mfConfig.numCols = dim[1];

		// Configure L, R tables. K+1 columns. For LTable, row i column K is
		// nnz ratings in row i of data matrix D.
		PsTableGroup.createDenseDoubleTable(LTableId, config.staleness,
				config.K + 1);
		PsTableGroup.createDenseDoubleTable(RTableId, config.staleness,
				config.K + 1);

		// Configure loss table
		LossRecorder.createLossTable();

	}

	@Override
	public void runWorkerThread(int threadId) {
		int numClients = PsTableGroup.getNumClients();
		int numThreads = PsTableGroup.getNumLocalWorkerThreads();
		mfConfig.numWorkers = numClients * numThreads;
		mfConfig.numThreads = numThreads;
		
		int workerRank = numThreads * config.clientId + threadId;
		
		if (workerRank == 0) {
			logger.info("Starting " + numClients + " nodes, each with "
					+ numThreads + " threads.");
		}

		MatrixFactWorker worker = new MatrixFactWorker(mfConfig, workerRank);
		worker.run();
	}

	public static void main(String[] args) {
		final MatrixFactConfig config = new MatrixFactConfig();
		final CmdLineParser parser = new CmdLineParser(config);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			logger.error(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		long trainTimeBegin = System.currentTimeMillis();

		MatrixFact mfThreads = new MatrixFact(config);
		mfThreads.run(config);

		if (config.clientId == 0) {
			long trainTimeElapsed = System.currentTimeMillis() - trainTimeBegin;
			logger.info("The program took " + trainTimeElapsed + " ms.");
		}

	}
}
