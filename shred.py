#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Takes a file to be shredded on HDFS.
Inspects the file to determine the underlying blocks on the Linux File System, and stores them in Zookeeper
Deletes the file from HDFS once it is inspected and scheduled for shredding.
Executes as a worker on each Datanode to shred the blocks in the Linux FS.
"""

import logging
import logging.handlers
import re
import subprocess
import sys

import argparse
from kazoo.client import KazooClient
from syslog_rfc5424_formatter import RFC5424Formatter

from .conf import conf

log = logging.getLogger('hdfshred')
log.setLevel(logging.DEBUG)
handler = logging.handlers.SysLogHandler(address='/dev/log')
handler.setFormatter(RFC5424Formatter())
log.addHandler(handler)


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Combines HDFS Delete with Linux Shred commands and audit logging to ensure file destruction"
    )
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {0}'.format(conf.VERSION))
    parser.add_argument('-m', '--mode', choices=('file', 'blocks'),
                        help="Specify mode; Delete a FILE from HDFS and add to the shred queue, "
                             "or find and shred all BLOCKS in the queue on this Datanode.")
    parser.add_argument('-f', '--filename', action="store", help="Specify a filename for the --shred file mode.")
    parser.add_argument('--debug', action="store_true", help='Increase logging verbosity.')
    log.debug("Parsing commandline args [{0}]".format(args))
    result = parser.parse_args(args)
    if result.debug:
        log.setLevel(logging.DEBUG)
    if result.mode is 'file' and result.filename is None:
        log.error("Argparse found a bad arg combination, posting info and quitting")
        parser.error("--mode 'file' requires a filename to register for shredding.")
    if result.mode is 'blocks' and result.filename:
        log.error("Argparse found a bad arg combination, posting info and quitting")
        parser.error("--mode 'blocks' cannot be used to register a new filename for shredding."
                     " Please try '--mode file' instead.")
    log.debug("Argparsing complete, returning args to main function")
    return result


def connect_zk(host):
    """create connection to ZooKeeper"""
    log.debug("Connecting to Zookeeper using host param [{0}]").format(host)
    zk = KazooClient(hosts=host)
    zk.start()
    if zk.state is 'CONNECTED':
        log.debug("Returning Zookeeper connection to main function")
        return zk
    else:
        raise "Could not connect to ZooKeeper with configuration string [{0}], resulting connection state was [{1}]"\
            .format(host, zk.state)


def run_shell_command(command):
    """Read output of shell command - line by line"""
    log.debug("Running Shell command [{0}]".format(command))
    # http://stackoverflow.com/a/13135985
    p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
    log.debug("Returning iterable to calling function")
    return iter(p.stdout.readline, b'')


def test_hdfs_available():
    """Checks if we can connect to HDFS"""
    return True


def check_hdfs_for_file(target):
    """Checks if file requested for shredding actually exists on HDFS."""
    # TODO: Run test for file existing in given HDFS path
    # "hdfs dfs -stat " + file_to_shred
    return True


def get_fsck_output(target):
    """Runs HDFS FSCK on the HDFS File to get block location information for Linux shredder"""
    fsck_out_iter = run_shell_command(['cat', 'sample-data.txt'])
    # "hdfs fsck " + file_to_shred + " -files -blocks -locations"
    return fsck_out_iter


def parse_blocks_from_fsck(raw_fsck):
    """Separate parser for FSCK output to make maintenance easier"""
    output = {}
    while True:
        try:
            current_line = raw_fsck.next()
            if current_line[0].isdigit():
                # TODO: Covert Block names to INTs and pack into job lots in ZK to reduce space
                output_split = current_line.split("[", 1)
                block_id = re.search(':(.+?) ', output_split[0]).group(1).rpartition("_")
                block_by_data_nodes = re.findall("DatanodeInfoWithStorage\[(.*?)\]", output_split[1])
                for block in block_by_data_nodes:
                    dn_ip = block.split(":", 1)
                    if dn_ip[0] not in output:
                        output[dn_ip[0]] = []
                    output[dn_ip[0]].append(block_id[0])
        except StopIteration:
            break
    return output


def write_blocks_to_zk(zk_conn, data):
    """Write block to be deleted to zookeeper"""
    # TODO: Keep tracked of deleted block / files
    count = 1
    while count < len(data):
        zk_path = "/shred/" + data[count]
        print zk_path
        zk_conn.ensure_path(zk_path)
        zk_path = zk_path + "/" + data[0]
        zk_conn.create(zk_path, makepath=True)
        count += 1


def read_blocks_from_zk(zk_conn, dn_id):
    """Read blocks to be deleted from Zookeeper"""
    output = {}
    return output


def generate_shred_task_list(block_dict):
    """Generate list of tasks of blocks to shred for this host"""
    output = {}
    return output


def shred_blocks(blocks):
    """Reliable shred process to ensure blocks are truly gone baby gone"""
    pass


def main():
    log.info("shred.py called with args [{0}]").format(sys.argv[1:])
    # Get invoke parameters
    log.debug("Parsing args using Argparse module.")
    args = parse_args(sys.argv[1:])
    # Checking the config was pulled in
    log.debug("Checking for config parameters.")
    if not conf:
        raise "Config from conf.py not found, please check configuration file is available and try again."
    # Test necessary connections
    log.debug("Checking if we can find the HDFS client and HDFS instance to connect to.")
    if not test_hdfs_available:
        raise "Could not find HDFS, please check the HDFS client is installed and HDFS is available and try again."
    # Test for Zookeeper connectivity
    log.debug("Looking for ZooKeeper")
    zk_host_connection_string = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
    zk = connect_zk(zk_host_connection_string)

    if args.mode is 'file':
        log.debug("Detected that we're running in 'File' Mode")
        # Test if target file exists on HDFS
        log.debug("Checking if file exists in HDFS")
        file_exists = check_hdfs_for_file(args.file_to_shred)
        if not file_exists:
            raise "File for shredding not found on HDFS: [{0}]".format(args.file_to_shred)
        # Get block information for the file we are to shred
        log.debug("Requesting FSCK information for file.")
        fsck_output = get_fsck_output(args.file_to_shred)
        log.debug("Requesting parser to return clean dict of block files to shred")
        # Parse HDFS fsck output into a dictionary of datanodes with lists of block IDs
        blocks_dict_out = parse_blocks_from_fsck(fsck_output)
        # Store blocks to be shredded into Zookeeper
        log.debug("Writing datanodes and blocks information to ZooKeeper for shredding workers")
        write_blocks_to_zk(zk, blocks_dict_out)
    elif args.mode is 'blocks':
        log.debug("Detected that we're running in Block Shredding worker mode")
        # Get my IP
        log.debug("Determinging this DataNode's IP")
        dn_id = "myip"
        # Get current blocks list from zk
        log.debug("Getting list of block shredding tasks for this Datanode")
        block_dict_in = read_blocks_from_zk(zk, dn_id)
        # Parse Block Dict into task list
        log.debug("Parsing block list into list of shredding tasks")
        shred_task_iter = generate_shred_task_list(block_dict_in)
        # Execute Shred Processor
        log.debug("Requesting shredding task execution")
        shred_blocks(shred_task_iter)
    else:
        raise "Bad operating mode [{0}] detected. Please retry by specifying mode of either 'file' or 'blocks'." \
            .format(args.mode)


if __name__ == "__main__":
    main()

