#!/usr/bin/env python

# Author: yihuaf
# 2015-6-16
if __name__ == '__main__':

  import os
  from os.path import dirname
  from os.path import join
  import time
  import argparse

  parser = argparse.ArgumentParser(description='Launches a JBosen application using YARN.')
  parser.add_argument('--client_jar_path', type=str, help='Path to the YARN client jar.')
  parser.add_argument('--app_master_jar_path', type=str, help='Path to the YARN application master jar.')
  parser.add_argument('--app_master_memory_size', type=int, default=350, help='Size of memory needed to run application master.')
  parser.add_argument('--num_nodes', type=int, default=1, help='Number of container nodes requested.');
  parser.add_argument('--container_memory', type=int, default=500, help='Size of memory needed to run each container.');
  parser.add_argument('--container_vcores', type=int, default=1, help='Number of cores requested for the container.');
  parser.add_argument('--priority', type=int, default=10, help='Priority of the JBosen app');
  parser.add_argument('--ps_app_jar_local_path', type=str, help='JBosen Application jar on local file system.')
  parser.add_argument('--ps_app_args', type=str,default="", help='JBosen Application other arguements needed')
  parser.add_argument('--ps_hdfs_prefix', type=str, default="", help='The HDFS director you want all the file to go to')

  args = parser.parse_args()

  params = {
      "app_master_jar":args.app_master_jar_path
      , "container_memory": args.container_memory
      , "container_vcores": args.container_vcores
      , "master_memory": args.app_master_memory_size
      , "priority": args.priority
      , "num_nodes": args.num_nodes
      , "ps_app_jar_local_path" : args.ps_app_jar_local_path
      , "ps_app_other_args" : '"' + args.ps_app_args + '"'
      , "ps_hdfs_dir_prefixs" : '"' + args.ps_hdfs_prefix + '"'
      }

  cmd = "hadoop jar "
  cmd += args.client_jar_path + " "
  cmd += "".join([" --%s %s" % (k,v) for k,v in params.items()])
  print cmd
  os.system(cmd)
