#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Configuration parameters for Shred client and server processors"""

VERSION = "0.0.4"

ZOOKEEPER = {
    'HOST': 'localhost',
    'PORT': 2181,
    'PATH': '/shred/'
}

HDFS_SHRED_PATH = "/tmp/shred"

LINUXFS_SHRED_PATH = ".shred"

# Duration in minutes
WORKER_SLEEP = 1