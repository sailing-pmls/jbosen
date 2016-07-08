### Building the YARN Client and Application Master ###
First, run

```
gradle buildYarn
```

under the project root directory. This will create the YARN client and application master jar files under `jbosen_yarn/build/libs/` directory named `yarnClient.jar` and `yarnApplicationMaster.jar`.

Or download the jar files directly from our repo:  
* [YARN Client 2.5.2](https://petuum.github.io/jbosen/resources/yarnClient-2.5.2.jar)

* [YARN Client 2.6.0](https://petuum.github.io/jbosen/resources/yarnClient-2.6.0.jar)

* [YARN Client 2.7.0](https://petuum.github.io/jbosen/resources/yarnClient-2.7.0.jar)

* [YARN ApplicationMaster 2.5.2](https://petuum.github.io/jbosen/resources/yarnApplicationMaster-2.5.2.jar)  

* [YARN ApplicationMaster 2.6.0](https://petuum.github.io/jbosen/resources/yarnApplicationMaster-2.6.0.jar)

* [YARN ApplicationMaster 2.7.0](https://petuum.github.io/jbosen/resources/yarnApplicationMaster-2.7.0.jar)

### Running on YARN ###
For a step by step instruction on a quick start to run SSP Demo, please follow [[here|Quick Start YARN]].   
For a step by step instruction on how to run Matrix Factorization app on YARN, please follow [[here|Matrix Factorization]].

For running apps on YARN, we have provided for you a running script `jbosen_yarn_run.py` under the root directory.    
Usage: `python scripts/jbosen_yarn_run.py --client_jar_path <Path to the yarnClient.jar> --app_master_jar_path <Path to the application master jar file> --ps_app_jar_local_path <Path to the ps app jar file>`    
These are the required command line arguments for running the ps app on YARN. For further usages, please consult with `python scripts/jbosen_yarn_run.py -h`or [[jbosen_yarn_run.py all command line arguments explained|jbosen_yarn_run.py command line arguments]].

### Extra Files ###
As part of the YARN requirement, files accessed by the app need to be on HDFS. Therefore, users need to put data files onto HDFS manually and pass the complete HDFS paths (with the format hdfs://\<domain\>/\<path\>) of the files as command line arguments.

### Things to watch out for ###
* YARN client, application master and JBösen app jar files need to be on local filesystem. The YARN client takes care of transferring the jar files into HDFS and containers.
* All other files that are accessed by the JBösen app need to be transferred to HDFS manually, as mentioned above.
* To access the application master and JBösen app log, use `yarn logs -applicationId <application id>` where the application id will be printed as part of the YARN client log.
* In order to use `--ps_hdfs_prefix <HDFS path>` flag correctly, the `<HDFS path>` needs to have r/w permission for both current login user and YARN user, if they are different. Otherwise, users may see permission related exceptions with YARN. 

### Related YARN and HDFS Command Line Tools and Functionalities ###

In the process of running the PS applications on YARN, there is no doubt some of the YARN and HDFS command line tools and functionalities will come in handy.

One of the most important identifier used by YARN to identify the running applications is the application id. When the YARN client is launched, it will print out the application id as part of the log. The application id will be in the format of `application_xxxxxxxxxxxxx_xxxx`.

* An alternative way to find out the application id:  
    * `yarn application -list`
    * This command will print out all running applications on YARN with their application ids.

* To obtain the logs from the Application Master and the containers:  
    * `yarn logs -applicationId <application id>`  

* To kill an application:
    *  `yarn application -kill <application id>`  
    Note: In most error cases, the YARN client and the Application Master will kill themselves. In the rare cases on misconfiguration or unexpected errno, the YARN client will not terminate the application. In situations like these, users will need this command in order to manually kill the running application. The users can then obtain logs and investigate the potential cause.

* To move data files to HDFS
    * `hadoop fs -copyFromLocal <src> <dst>`
    * Some apps require users to manually move data files from local file system onto HDFS. This command line tools can help to achieve this task.
