package org.petuum.app.matrixfact;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixFact {
	private static final Logger logger = LoggerFactory
			.getLogger(MatrixFact.class);

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

		MatrixFactWorker worker = new MatrixFactWorker(config);
		worker.run(config);

		if (config.clientId == 0) {
			long trainTimeElapsed = System.currentTimeMillis() - trainTimeBegin;
			logger.info("The program took " + trainTimeElapsed + " ms.");
		}

	}
}
