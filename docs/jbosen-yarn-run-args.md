# jbosen_yarn_run.py Command-line Arguments

## Required Arguments
* `--client_jar_path`
  * The path to `yarnClient.jar`.
  * Default path is: `jbosen_yarn/build/libs/yarnClient.jar`
* `--app_master_jar_path`
  * The path to `yarnApplicationMaster.jar
  * Default path is : `jbosen_yarn/build/libs/yarnApplicationMaster.jar`
* `--ps_app_jar_local_path`
  * The path to the PS application jar that users want to run.
  * SSPDemo default path: `app/ssp_demo/build/libs/SSPDemo.jar`
  * MatrixFact default path: `app/matrixfact/build/libs/MatrixFact.jar`

## Optional Arguments
* `--app_master_memory_size`
  * The memory size required to run the application master.
  * Default value: 350 MB
* `--num_nodes`
  * The number of nodes required by the application.
  * Default value: 1
* `--container_memory`
  * The memory size required for each container to run.
  * Default value: 500 MB
* `--container_vcores`
  * The number of virtual core required for each containers.
  * Default value: 1
* `--priority`
  * The priority of the ps app.
  * Default value: 10
* `--ps_app_other_args`
  * All the extra command line arguments to be passed into the ps app.
  * Required to use double quote around the string in order to be parsed correctly.
* `--ps_hdfs_prefix`
  * The YARN client and application master will write all files into this folder on HDFS.
  * Require the full path: `hdfs://<domain>/<path to dir>`
  * Default value: Write to `/user/$USER/` directory.
 
