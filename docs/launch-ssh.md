# Launch Using SSH

## Host file
For non-YARN operation, JBösen requires a host file, containing the IP addresses and ports of the machines that JBösen should run on. Each line of the host file should be in the format of `<ip>:<port>`. Below is a sample host file:
```
192.168.1.101:29000
192.168.1.102:29000
```
Please make sure that the port users specify for the Petuum does not conflict with other processes running on the cluster. We recommend using ports between 20000 and 30000.

## SSH launch script
For your convenience, we have provided a generic launch script `bosen_run.py` under `scripts/`. Usage:
```
python scripts/jbosen_run.py <path to host file> <app jar file path> <app main class name>
```
For additional command line argument options, please see `python scripts/jbosen_run.py -h` as well as [this guide](jbosen-run-args.md). To kill stray JBösen processes after running, please see [here](killing.md).

The launch script provided assumes that password-less ssh is enabled to each of the ip addresses listed.

## SSP Demo
Run the following: 
 
1. `gradle buildSSPDemo`
2. Prepare a host file as explained above. You may also use the simple host file at `machinefile/sample_machinefile`, which simulates running two hosts on your local machine.
3. `python scripts/jbosen_run.py <path to host file> app/ssp_demo/build/libs/SSPDemo.jar org.petuum.app.ssp_demo.SSPDemo`
