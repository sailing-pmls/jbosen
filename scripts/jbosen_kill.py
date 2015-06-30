#!/usr/bin/env python

if __name__ == '__main__':
  
  import argparse
  import os
  import time
  
  parser = argparse.ArgumentParser(description='Kills a JBosen application.')
  parser.add_argument('host_file', type=str, help='Path to the host file to use.')
  parser.add_argument('main_class', type=str, help='Fully qualified class name that contains the main method.')
  args = parser.parse_args()

  with open(args.host_file, 'r') as f:
    host_ips = [line.split(':')[0] for line in f]

  for client_id, ip in enumerate(host_ips):
    cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ' + ip + ' '
    cmd += '\'pkill -f "^java .*' + args.main_class + '"\''
    print(cmd)
    os.system(cmd)

