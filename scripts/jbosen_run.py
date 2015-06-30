#!/usr/bin/env python

if __name__ == '__main__':
  
  import argparse
  import os
  import time
  
  parser = argparse.ArgumentParser(description='Launches a Java JBosen application.')
  parser.add_argument('host_file', type=str, help='Path to the host file to use.')
  parser.add_argument('class_path', type=str, help='Java classpath that contains the application.')
  parser.add_argument('main_class', type=str, help='Fully qualified class name that contains the main method.')
  parser.add_argument('--num_local_worker_threads', type=int, default=1, help='Number of application worker threads per client.')
  parser.add_argument('--num_local_comm_channels', type=int, default=1, help='Number of network channels per client.')
  parser.add_argument('--java_args', type=str, default='', help='Extra arguments to pass to Java.')
  parser.add_argument('--app_args', type=str, default='', help='Extra arguments to pass to the application.')
  args = parser.parse_args()

  with open(args.host_file, 'r') as f:
    host_ips = [line.split(':')[0] for line in f]

  def kill(client_id, ip):
    cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ' + ip + ' '
    cmd += '\'pkill -f "^java .*' + args.main_class + '"\''
    print(cmd)
    os.system(cmd)

  def launch(client_id, ip):
    cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ' + ip + ' '
    cmd += '"cd ' + os.getcwd() + '; '
    cmd += 'java ' + args.java_args + ' '
    cmd += '-cp ' + args.class_path + ' '
    cmd += args.main_class + ' '
    cmd += '-clientId %d' % client_id + ' '
    cmd += '-hostFile %s' % args.host_file + ' '
    cmd += '-numLocalWorkerThreads %d' % args.num_local_worker_threads + ' '
    cmd += '-numLocalCommChannels %d' % args.num_local_comm_channels + ' '
    cmd += args.app_args + '" &'
    print(cmd)
    os.system(cmd)

  print("Killing previous instances of the application...")
  for client_id, ip in enumerate(host_ips):
    kill(client_id, ip)

  print("Starting new instances of the application...")
  for client_id, ip in enumerate(host_ips):
    launch(client_id, ip)
