# Killing All JBosen-related Processes

In the case that users need to kill all stray JBosen processes on local clusters, here are the steps.

We have provided the users with a script under `scripts/jbosen_kill.py`.  
The usage:  
* `python scripts/jbosen_kill.py <path to host file> <app main class>`

The host file and the app main class should be the same ones the user used to run the JBosen app.

In the case that users need to manually kill the process on each machine, simply use:  
* `pkill -f "^java .*org.petuum.app"`
