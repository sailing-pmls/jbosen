# jbosen_run.py Command-line Arguments

## Required Arguments ##
* host_file
    The path to the host file to use.
* class_path
    The java class path to the Bosen application.
* main_class
    The fully qualified class name of the Bosen application that contains the main method.

## Optional Arguments ##
* --num_local_worker_threads  
    Number of application worker threads per client.  
    Default value: 1  
* --num_local_comm_channels  
    Number of network channels per client.  
    Default value: 1  
* --java_args  
    Extra arguments to pass to Java Virtual Machine.  
    Default value: <empty>
* --app_args  
    Extra arguments to pass to the Bosen application.  
    Default value: <empty>
