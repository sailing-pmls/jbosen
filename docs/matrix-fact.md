## Matrix Factorization App ##
The Matrix Factorization App decomposes an N by M matrix D into two lower-rank matrices L and R where L * R = D, where L and R have sizes N by K and K by M (for a user-specified K). The algorithm used in this app is the Stochastic Gradient Descent (SGD) algorithm to optimize an objective function by applying gradient descent to random subsets of the data.
## Running ##

#### Running Matrix Factorization on Local Clusters ####
1. Read and follow [[this|Building JBosen]] section of the Wiki. 
2. Run:  
    `gradle buildMatrixFact`
3. Prepare a host file. Each line in the file is of the format: `<ip address>:<port>`.
4. Prepare a data file.
    We have prepared a data file generator: `app/matrixfact/scripts/generate_data.py`.    
    Please follow [[this guide|Test Data Generator]] to prepare a data file.
5. Run:  
    `python scripts/jbosen_run.py <path to host file> app/matrixfact/build/libs/MatrixFact.jar org.petuum.app.matrixfact.MatrixFact --app_args "-dataFile <path to data file>"`  
    If you haven't prepared a host file, you may use `machinefile/sample_machinefile` to simulate two hosts on your local machine.


Besides the required command line argument `-dataFile` for the app, the Matrix Factorization app also offers other optional command line arguments for further configuration.

* `-staleness`  
    Staleness of parameter tables.  
    Default value: 0  
* `-numEpochs`  
    Number of passes over data.  
    Default value: 10  
* `-K`  
    Rank of factor matrices.  
    Default value: 20
* `-lambda`  
    Regularization parameter lambda.  
    Default value: 0.1
* `-learningRateDecay`  
    Learning rate is multiplied by this factor each iteration.  
    Default value: 1
* `-learningRateEta0`  
    Learning rate parameter. If you are getting garbage output or you see the objective value increase rapidly, try lowering this parameter by 10x or more.  
    Default value: 0.001  
* `-numMiniBatchesPerEpoch`  
    Equals to number of clock() calls per data sweep.  
    Default value: 1
* `-outputPrefix`  
    Output to outputPrefix.L, outputPrefix.W.  
    Default value: Does not output.
* `-numLocalWorkerThreads`  
    Number of worker threads to use per host/machine.  
    Default value: 1

To use these, simply add them onto the --app_args argument. For example:  
`python scripts/jbosen_run.py ... --app_args "-dataFile <path to data file> -numEpoch 10 -staleness 0 ... -outputPrefix output"`.

#### Running Matrix Factorization on YARN ####

1. Read and follow [[this|Building JBosen]] section of the Wiki.
2. Read and follow the section above to make sure the Matrix Factorization app runs correctly on the local machine.
3. From step 2, users should have gradle build the Matrix Factorization app jar file under `app/matrixfact/build/libs/MatrixFact.jar` and prepared a data file for the app.
4. Move the the data file onto HDFS by running:  
`hadoop fs -copyFromLocal <path to data file on local> <a directory on HDFS>`  
Make sure the directory on HDFS has the appropriate permission for the user and YARN. 
5. Run:  
    `gradle buildYarn`   
    This should create two jar files, yarnClient.jar and yarnApplicationMaster.jar under `jbosen_yarn/build/libs`.
6. Run:  
    `python scripts/jbosen_yarn_run.py --client_jar_path jbosen_yarn/build/libs/yarnClient.jar --app_master_jar_path jbosen_yarn/build/libs/yarnApplicationMaster.jar --ps_app_jar_local_path app/matrixfact/build/libs/MatrixFact.jar --ps_app_args "-dataFile <Path to data file on HDFS> -outputPrefix <Prefix of output files on HDFS>" --num_nodes X`  
    where `X` is the number of nodes (machines) to use.

    **Note: HDFS paths must be in this format: `hdfs://<domain>/<path>`**

7. Record the application id from the logs.

8. Obtain the results by running:  
    `yarn logs -applicationId <application id>`

Furthermore, as explained in "Running Matrix Factorization on Local Clusters" section, the Matrix Factorization app can take extra optional arguments. Users can simply pass these arguments in `--ps_app_args`. Note that all HDFS paths provided to the app need to be in the following format of `hdfs://<domain>/<path>`.

In addition to these optional arguments, the JBösen system can be configured using extra command line arguments.  When running on local machines, the jbosen_run.py scripts takes care of these JBösen system configurations. However, when using YARN, users needs to pass in these system arguments as part of the --ps_app_args. A detailed explanation on these arguments are [[here|JBosen system command line configuration arguments]].

**Note: when using `-numLocalWorkerThreads` to set the number of worker threads on each of the client as well as the `-numLocalCommChannels` option, be sure to change the option `--container_vcores` to an appropriate number.**
