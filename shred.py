#!/usr/bin/python
'''
Get list of blocks needed to be shredded
'''

import os
import re
import subprocess

#output='sample-data.txt'

bashCommand = ['cat', 'sample-data.txt']

def run_command(command):
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
  '''
  outputSplit = currentLine.split("[",1)
  blockId = re.search(':(.+?) ', outputSplit[0]).group(1).rpartition("_")
  datanodes.append(blockId[0])
  blockByDatanodes = re.findall("DatanodeInfoWithStorage\[(.*?)\]",outputSplit[1])
  for block in blockByDatanodes:
    tmp = block.split(":",1)
    datanodes.append(tmp[0])
  return datanodes


def main():
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
    except StopIteration:
      break
  print info


if __name__ == "__main__":
  main()

