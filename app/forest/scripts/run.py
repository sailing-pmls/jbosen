#!/usr/bin/env python

import os
from os.path import dirname
from os.path import join
import time

app_dir = dirname(dirname(os.path.realpath(__file__)))
proj_dir = dirname(dirname(app_dir))
host_file_name = "machinefile/hostfile"

jar_file = join(app_dir, "build", "libs", "batch_mlr-all.jar")
host_file = join(proj_dir, host_file_name)


# Get host IPs
with open(host_file, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split(":")[0] for line in hostlines]


ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=~/.ssh/known_hosts "
    )
#for ip in host_ips:
#  cmd = ssh_cmd + ip + " pkill -6 -f org.petuum.app"
#  os.system(cmd)
#print "Done killing"
#Note: your data files should be in the format data.11, data.12, data.21, data.23, data.31, data.32 if you have three nodes
#      with two threads each
cmd_args = '-dataFile "data" -hostFile "../../machinefile/hostfile" -staleness 1 -numLocalWorkerThreads 8 -lambda 0.01 '
cmd_args += '-numFeatures 8 -numClasses 3 -learningRateDecay 0.95 -learningRate 0.01 -outputFile "output/model.csv" -numEpochs 100'

for client_id, ip in enumerate(host_ips):
  if ip:
    print "Running worker", str(client_id), "on", ip
    cmd = ssh_cmd + ip
    cmd += " 'cd ~/private/petuum/petuum-java/app/forest/; "
    cmd += 'java -jar build/libs/forest-all.jar -depth 2 -dataFile test_data/reg -numFeatures 2 -hostFile "../../machinefile/hostfile" -numClasses 4 -numLocalWorkerThreads 8 -outputFile "model/out" -regression 1' 
    cmd += " -clientId "+ str(client_id) + " >> log/log" + str(client_id) +"'" 
    cmd += "&"
    print cmd
    os.system(cmd)

  #if client_id == 0:
  #  print "Waiting for first client to set up"
  #  time.sleep(2)

print "DONE"
