# YARN Quickstart

1. Read and follow the [requirement](https://github.com/petuum/petuum-java/wiki/Requirements) section of the Wiki. 

2. cd into the petuum-java directory, and run:

    `gradle buildSSPDemo`

3. Verify petuum-java is working in SSH mode:

    `python scripts/jbosen_run.py machinefile/sample_machinefile app/ssp_demo/build/libs/SSPDemo.jar org.petuum.app.ssp_demo.SSPDemo --num_local_worker_threads 2 --num_local_comm_channels 1 --app_args "-numIterations 10 -staleness 2" `

    Note: We have included a sample host file under `machinefile/sample_machinefile` using the localhost. Users should set up password-less ssh to localhost on their machine.

4. Hadoop YARN only, run: 

    `gradle buildYarn`

5. Verify petuum-java is working on YARN using 2 machines:

    `python scripts/jbosen_yarn_run.py --client_jar_path jbosen_yarn/build/libs/yarnClient.jar --app_master_jar jbosen_yarn/build/libs/yarnApplicationMaster.jar --ps_app_jar_local_path app/ssp_demo/build/libs/SSPDemo.jar --ps_hdfs_prefix hdfs://<hdfs_domain>/<hdfs_path> --num_nodes 2`

    where `<hdfs_domain>` is the hostname of your HDFS filesystem, and `<hdfs_path>` is a path to a HDFS directory.

    NOTE: the current login user, and the user "yarn", must have read/write permissions to `<hdfs_path>`. Example: if you are running "python scripts/jbosen_yarn_run.py ..." as "root", then both "root" and "yarn" need permissions to `<hdfs_path>`.
