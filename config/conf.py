#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Configuration parameters for Shred client and server processors"""

VERSION = "0.0.4"

TEST_MODE = True
LOG_LEVEL = "DEBUG"

ZOOKEEPER = {
    'HOST': 'localhost',
    'PORT': 2181,
    'PATH': '/shred/'
}

HDFS_SHRED_PATH = "/tmp/shred"

LINUXFS_SHRED_PATH = ".shred"

# Duration in minutes
# Worker wait is delay between checks of worker activity
WORKER_WAIT = 1
# Leader wait is how long the each leader should wait for workers to complete distributed tasks
LEADER_WAIT = 15