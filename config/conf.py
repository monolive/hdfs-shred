#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Configuration parameters for Shred client and server processors"""

VERSION = "0.0.4"

TEST_MODE = True
LOG_LEVEL = "DEBUG"

HDFS_ROOT = '/hadoop/hdfs/data'

ZOOKEEPER = {
    'HOST': 'localhost',
    'PORT': 2181,
    'PATH': '/testshred/'
}

HDFS_SHRED_PATH = "/tmp/testshred"

LINUXFS_SHRED_PATH = ".testshred"

# Number of times to overwrite the file before writing out zeros and removing it from the filesystem
# a SHRED_COUNT of 6 will overwrite the file 7 times; 6 with random garbage, and the 7th as zeros.
SHRED_COUNT = 6

# Duration in minutes
# Worker wait is delay between checks of worker activity
WORKER_WAIT = 1
# Leader wait is how long the each leader should wait for workers to complete distributed tasks
LEADER_WAIT = 15