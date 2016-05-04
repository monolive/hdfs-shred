#!/usr/bin/python
'''
Get list of blocks needed to be shredded
'''

from kazoo.client import KazooClient
import re
import subprocess

#output='sample-data.txt'

bashCommand = ['cat', 'sample-data.txt']
zkHost="127.0.0.1:2181"

def run_command(command):
  '''
  Read output of command - line by line
  '''
  p = subprocess.Popen(command,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT)
  return iter(p.stdout.readline, b'')

def get_info(currentLine):
  datanodes = []
  '''
  Split line on first square bracket
  Before - blockId + stuff
  After  - Datanodes IP + stuff
  TODO: Write info to log file and create record of deleted block
  '''
  outputSplit = currentLine.split("[",1)
  blockId = re.search(':(.+?) ', outputSplit[0]).group(1).rpartition("_")
  datanodes.append(blockId[0])
  blockByDatanodes = re.findall("DatanodeInfoWithStorage\[(.*?)\]",outputSplit[1])
  for block in blockByDatanodes:
    tmp = block.split(":",1)
    datanodes.append(tmp[0])
  return datanodes

def connection_zk(host):
  '''
  create connection to ZooKeeper
  TODO: Add logging and check connectivity
  '''
  zk = KazooClient(hosts=host)
  zk.start()
  return zk

def write_to_zk(zkConnection,data):
  '''
  Write block to be deleted to zookeeper
  TODO: Keep tracked of deleted block / files
  '''
  count = 1
  while count < len(data):
    zk_path = "/shred/" + data[count]
    print zk_path
    zkConnection.ensure_path(zk_path)
    zk_path = zk_path + "/" + data[0]
    zkConnection.create(zk_path,makepath=True)
    count += 1

def main():
  zk = connection_zk(zkHost)
  output = run_command(bashCommand)
  while True:
    try:
      currentLine = output.next()
      '''
      Check if line is starting by a number
      0. BP-1420600807-172.16.0.30-1452120396237:blk_1073839025_116619 len=134217728 repl=3 [DatanodeInfoWithStorage[172.16.0.40:1019,DS-589158a2-cacf-4993-92ab-0c08f9a092de,DISK], DatanodeInfoWithStorage[172.16.0.50:1019,DS-55dae783-c3df-4c65-bf76-e42d970a8d55,DISK], DatanodeInfoWithStorage[172.16.0.80:1019,DS-7cfc6d5c-9e7a-4d49-831d-81d92109d193,DISK]]
      '''
      if currentLine[0].isdigit():
        info = get_info(currentLine)
        write_to_zk(zk,info)

    except StopIteration:
      break

if __name__ == "__main__":
  main()

