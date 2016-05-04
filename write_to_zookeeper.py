#!/usr/bin/python
'''
Write to ZooKeeper
'''

from kazoo.client import KazooClient
import socket
#import logging
#logging.basicConfig()

data = ['blk_1073839032', '172.16.0.70', '172.16.0.60', '172.16.0.80']

zkHost="127.0.0.1:2181"

def main():
  # Establish connection to server
  zk = KazooClient(hosts=zkHost)
  zk.start()
  count = 1
  zk.ensure_path("/shred")

  while count < len(data):
    zk_path = "/shred/" + data[count]
    # print zk_path
    zk.ensure_path(zk_path)
    zk_path = zk_path + "/" + data[0]
    zk.create(zk_path, data[0])
    count += 1

  children = zk.get_children("/shred")
  print children

if __name__ == "__main__":
  main()
