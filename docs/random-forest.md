# Random Forest
A [Random Forest] is a classification algorithm that uses a large number of decision trees. In our Random Forest app implemented on **JBösen**, it generates an M-sized set of decision trees of depth D, where each tree is trained on a size N subset of the datapoints by bootstrap sampling. Both classification and regression are supported, where classification is performed by taking the majority vote of the trees and regression is performed by averaging the trees. Each tree is trained with a simplified version of [C4.5](http://en.wikipedia.org/wiki/C4.5_algorithm)(no pruning or handling missing values) for classification and CART with greedy minimization of sum of squared error for regression.

Output is in CSV form, in which each tree is represented by two rows. The first row denotes which feature is split on (offset by +1), with the placeholder value of -1 if it is a leaf and placeholder value of 0 if the node does not exist. The second row denotes the value of the split if it is not a leaf, or the output if it is a leaf. There is a blank line in between each tree.

Note that staleness does not play a role in this application for it is not iterative.

## Running Random Forest on Local Clusters

1. Read and follow [this](http://docs.petuum.com/projects/petuum-jbosen/en/latest/building.html) section of the Wiki. 
2. Run:  
    `gradle forest`
3. Prepare a host file. Each line in the file is of the format: `<ip address>:<port>`.
4. Prepare a data file.
    The input data needs to be in the libsvm format(We default that the index of the features starts from 1).
5. Run:  
    ```
    python scripts/jbosen_run.py <path to host file> app/forest/build/libs/forest-all.jar 
    
    org.petuum.app.forest.Forest --app_args "-dataFile <path to data file> -numFeatures 100"
    ```
    
    If you haven't prepared a host file, you may use `machinefile/sample_machinefile` to simulate two hosts on your local machine.


Besides the required command line argument `-dataFile` for the app, the Random Forest app also offers other optional command line arguments for further configuration.

* `-dataFile`  
    Path to data file.
    Required
* `-numFeatures`  
    Number of features per datapoint.  
    Required
* `-numTrees`  
    Number of trees totally. It should be some times of #workers(#Clients * #LocalWorkerThreads).
    Default value: 10
* `-numClasses`  
    Number of classes/labels. Only applicable for classification.
    Default value: 2  
* `-lossFile`  
    Location of file containing outputted loss values.
    Default value: "log/loss"  
* `-hdfs`  
    Boolean flag to use HDFS for file io rather than the normal file ysstem.  
    Default value: false. If you use the flag -hdfs, the app will use HDFS.
* `-sparse`  
    Boolean flag to use sparse rows to store data or not(use dense rows to store data).
    Default value: false. If you use the flag -sparse, the app will use sparse rows to store data.
* `-outputFile`  
    Output to outputFile.  
    Default value: empty string
* `-depth`
    Maximum depth of trees created. A tree with only one node has depth of 0.
    Default value: 4
* `-regression`
    Boolean flag to use regression or classification.
    Default value: false. If you use the flag -regression, the app will construct regression forest. 
* `-subSample`  
    Bootstrap sampling rate of each tree.
    Default value: 1.0
* `-dataFileT`  
    Path to testing data file. If provided, the app outputs the accuracy on the testing data; otherwise, the app outputs the accuracy of the training process.
    Default value: empty string
* `-labelIndexFrom`  
    Starting index of the labels. Only applicable for classification. For example, if the labels of your data are like 1,2,..,10, you should set the argument to 1. If the labels of your data are like -1,0,1, you should set the argument to -1. If the labels of your data are like -1,1, you'd better pre-process the labels to 0,1 or -1,0 so that the random forest app can produce better results.
    Default value: 0
* `-maxFeatures`  
    The number of features to consider when looking for the best split at each node. They are randomly selected from all the features. You can set it to "sqrt", "log2", "all", an integer or a float number. For example, if numFeatures is set to 100, the maxFeatures will be: 
    If "sqrt", then maxFeatures = round(sqrt(100)) = 10.
    If "log2", then maxFeatures = round(log2(100)) = round(6.644) = 7.
    If "all", then maxFeatures = 100.
    If int, then maxFeatures = (int) maxFeatures.
    If float, then consider round(maxFeatures * 100) features at each split.
    Default value: "sqrt"

To use these, simply add them onto the --app_args argument. For example: if you want to test on the widely used dataset [mnist.scale](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist.scale.bz2) and [mnist.scale.t](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist.scale.t.bz2), you can `wget` these sets and then `bzip2 -d` them respectively. After that, you can just type the command below and wait for results:
```
python scripts/jbosen_run.py machinefile/sample_machinefile app/forest/build/libs/forest-all.jar 
 
org.petuum.app.forest.Forest --app_args "-dataFile mnist.scale -numClasses 10 -numFeatures 784 
 
-depth 16 -numTrees 50 -dataFileT mnist.scale.t"
```

## Running Random Forest on YARN

1. Read and follow [this](http://docs.petuum.com/projects/petuum-jbosen/en/latest/building.html) section of the Wiki.
2. Read and follow the section above to make sure the Random Forest app runs correctly on the local machine.
3. From step 2, users should have gradle build the Random Forest app jar file under `app/forest/build/libs/forest-all.jar` and prepared a data file for the app.
4. Move the the data file onto HDFS by running:  
`hadoop fs -copyFromLocal <path to data file on local> <a directory on HDFS>`  
Make sure the directory on HDFS has the appropriate permission for the user and YARN. 
5. Run:  
    `gradle buildYarn`   
    This should create two jar files, yarnClient.jar and yarnApplicationMaster.jar under `jbosen_yarn/build/libs`.
6. Run:  
    ```
    python scripts/jbosen_yarn_run.py --client_jar_path jbosen_yarn/build/libs/yarnClient.jar
    
    --app_master_jar_path jbosen_yarn/build/libs/yarnApplicationMaster.jar --ps_app_jar_local_path 
    
    app/forest/build/libs/forest-all.jar --ps_app_args "-dataFile <Path to data file on HDFS> ... 
    
    -outputFile <output file on HDFS>" --num_nodes X
    ```
    
    where `X` is the number of nodes (machines) to use.

    **Note: HDFS paths must be in this format: `hdfs://<domain>/<path>`**

7. Record the application id from the logs.

8. Obtain the results by running:  
    `yarn logs -applicationId <application id>`

Furthermore, as explained in "Running Random Forest on Local Clusters" section, the Random Forest app can take extra optional arguments. Users can simply pass these arguments in `--ps_app_args`. Note that all HDFS paths provided to the app need to be in the following format of `hdfs://<domain>/<path>`. For the mnist example, you can use the command below to test on yarn:
```
python scripts/jbosen_yarn_run.py --client_jar_path jbosen_yarn/build/libs/yarnClient.jar 

--app_master_jar_path jbosen_yarn/build/libs/yarnApplicationMaster.jar --ps_app_jar_local_path 

app/forest/build/libs/forest-all.jar --ps_app_args "-dataFile hdfs://<domain>/<pathToMnist> 

-numClasses 10 -numFeatures 784 -depth 16 -numTrees 50 -dataFileT hdfs://<domain>/<pathToMnistT> 

-hdfs -lossFile hdfs://<domain>/<pathToLossFile>" --num_nodes 2
```

In addition to these optional arguments, the JBösen system can be configured using extra command line arguments.  When running on local machines, the jbosen_run.py scripts takes care of these **JBösen** system configurations. However, when using YARN, users needs to pass in these system arguments as part of the --ps_app_args. A detailed explanation on these arguments are [here](http://docs.petuum.com/projects/petuum-jbosen/en/latest/cmdline-args.html).

**Note: when using `-numLocalWorkerThreads` to set the number of worker threads on each of the client as well as the `-numLocalCommChannels` option, be sure to change the option `--container_vcores` to an appropriate number.**
    
## Benchmark

We have tested our random forest app on some large datasets. For example, to handle the [HIGGS](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/HIGGS.bz2) dataset, we use 4 machines, each with 16 threads, to train 640 trees. That's to say, each thread trains 10 trees. When we set `samSample` to 0.1 and set `depth` to 16, it takes about 10 minutes to train and test the dataset, and the AUC score is 0.8055. When we set `samSample` to 0.2 and set `depth` to 18, the whole process takes about 20 minutes, and the AUC score is 0.8116. These results are similar to the gradient boosted tree results mentioned in the [paper](https://arxiv.org/pdf/1402.4735v2.pdf). Therefore, we think our app is a good choice for you if you want to handle a problem fast and accurately by using random forest.

    
[Random Forest]: http://www.stat.berkeley.edu/~breiman/RandomForests/cc_home.htm
