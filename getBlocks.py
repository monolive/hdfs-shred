#!/usr/bin/python

import subprocess
import argparse

def readArguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-f','--file', action="store", dest="file", required=True, help="File to shred")
  args = parser.parse_args()
  return args

def checkFile(file):
  '''
  Check if file exist in HDFS
  '''
  command = "hdfs fsck -stat " + file
  subprocess.check_call(command)


def getBlocks(file):
  '''
  Get list of blocks 
  '''
  command = "hdfs fsck " + file + " -files -blocks -locations"
  run_command(command)

def run_command(cmd):
  print cmd

def main():
  args = readArguments()
  checkFile(args.file)


if __name__ == "__main__":
  main()