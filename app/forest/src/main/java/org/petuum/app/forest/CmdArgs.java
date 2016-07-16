package org.petuum.app.forest;
import org.petuum.jbosen.PsConfig;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class CmdArgs extends PsConfig {

    @Option(name = "-numFeatures", required = true,
            usage = "number of features for dataset")
    public int numFeatures;


    @Option(name = "-numClasses", required = false,
            usage = "number of class labels. Default = 2")
    public int numClasses = 2;

    @Option(name = "-numTrees", required = false,
            usage = "number of trees totally. Default = 10")
    public int numTrees = 10;

/*    @Option(name = "-staleness", required = false,
            usage = "Staleness of parameter tables. Default = 0")*/
    public int staleness = 0;


    @Option(name = "-dataFile", required =true,
            usage = "Path to data file.")
    public String dataFile = "";

    @Option(name = "-subSample", required =false,
            usage = "Bootstrap sampling rate of each tree. Default is 1.0")
    public float subSample = 1.0f;

    @Option(name = "-dataFileT", required =false,
            usage = "Path to testing data file. Default is same as dataFile")
    public String dataFileT = "";
    
    @Option(name = "-outputFile", required = false,
            usage = "Path to model output file. A value of \"\" "
                    + "will result in no model being output. Default is \"\"")
    public String outputFile = ""; 

    @Option(name = "-depth", required = false,
            usage = "Maximum depth of trees created. A tree with one split has depth 0. Default is 0")
    public int depth = 4;

    @Option(name = "-regression", required = false,
            usage = "If true, regression trees will be used, otherwise decision trees will be used. Default is false")
    public boolean regression = false;

    @Option(name = "-hdfs", required = false,
            usage = "True if Hadoop Distributed File System is used. False if the local filesytem is used. Default is false")
    public boolean hdfs = false; 

    @Option(name = "-sparse", required = false,
            usage = "True if the data is sparse. Default is false")
    public boolean sparse = false;
    
    @Option(name = "-lossFile", required = false,
            usage = "Path to loss log file. Default is \"log/loss\"")
    public String lossFile = "log/loss"; 

    public boolean featureOneBased = true; 

    @Option(name = "-labelIndexFrom", required = false,
            usage = "Starting index of the labels. Default is 0")
    public int labelIndexFrom = 0; 

    @Option(name = "-maxFeatures", required = false,
            usage = "The number of features to consider when looking for the best split. You can set it \"log2\", \"all\", an integer or a float number. Default is \"sqrt\"")
    public String maxFeatures = "sqrt"; 

    public CmdArgs(String[] args)
    {
        parse(args);
    }
}


