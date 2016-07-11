# Command-line Arguments

These command line arguments are used to configure the JBosen system. For running the out of box apps on local machines, the jbosen_run.py script takes care of these arguments. For running the out of box app on YARN, these arguments are being passed into the app as part of the `--ps_app_args` argument through the jbosen_yarn_run.py script. 

* `clientId`  
    * ID of this particular client.  
* `-hostFile`  
    * The path to host file.  
* `-numLocalWorkerThreads`  
    * The number of worker threads per client.  
    * Default value: 1  
* `-numLocalCommChannels`  
    * The number of communication channels per client. The recommend value is 1 per 4-8 worker threads.  
    * Default value: 1  
