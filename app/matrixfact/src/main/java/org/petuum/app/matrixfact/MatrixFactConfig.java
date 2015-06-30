package org.petuum.app.matrixfact;

import org.kohsuke.args4j.Option;
import org.petuum.jbosen.PsConfig;

public class MatrixFactConfig extends PsConfig {
	@Option(name = "-staleness", required = false, usage = "Staleness of parameter tables. Default = 0")
	public int staleness = 0;

	@Option(name = "-dataFile", required = true, usage = "Path to data file.")
	public String dataFile = "";

	@Option(name = "-numEpochs", required = false, usage = "Number of passes over data. Default = 10")
	public int numEpochs = 10;

	@Option(name = "-K", required = false, usage = "Rank of factor matrices. Default = 20")
	public int K = 20;

	@Option(name = "-lambda", required = false, usage = "Regularization parameter lambda. Default = 0.1")
	public double lambda = 0.1f;

	@Option(name = "-learningRateDecay", required = false, usage = "Learning rate parameter. Default = 1")
	public double learningRateDecay = 1f;

	@Option(name = "-learningRateEta0", required = false, usage = "Learning rate parameter. Default = 0.001")
	public double learningRateEta0 = 0.001f;

	@Option(name = "-numMiniBatchesPerEpoch", required = false, usage = "Equals to number of clock() calls per data sweep. "
			+ "Default = 1")
	public int numMiniBatchesPerEpoch = 1;

	@Option(name = "-outputPrefix", required = false, usage = "Output to outputPrefix.L, outputPrefix.W.")
	public String outputPrefix = "";
}
