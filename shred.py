#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Proof of Concept Hadoop to shred files deleted from HDFS for audit compliance.
See https://github.com/Chaffleson/hdfs-shred
"""

import logging
import logging.handlers
from syslog_rfc5424_formatter import RFC5424Formatter
import re
import subprocess
import sys
import argparse
import pickle
from zlib import compress, decompress
from kazoo.client import KazooClient

from config import conf

# Set to True to enhance logging when working in a development environment
test_mode = True

log = logging.getLogger(__file__)
log.setLevel(logging.INFO)
handler = logging.handlers.SysLogHandler(address='/dev/log')
handler.setFormatter(RFC5424Formatter())
log.addHandler(handler)
if test_mode:
    con_handler = logging.StreamHandler()
    log.addHandler(con_handler)


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Proof of Concept Hadoop to shred files deleted from HDFS for audit compliance."
    )
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {0}'.format(conf.VERSION))
    parser.add_argument('-m', '--mode', choices=('client', 'worker', 'shredder'),
                        help="Specify mode; 'client' submits a --filename to be deleted and shredded, "
                             "'worker' triggers this script to represent this Datanode when deleting a file from HDFS, "
                             "'shredder' triggers this script to check for and shred blocks on this Datanode")
    parser.add_argument('-f', '--filename', action="store", help="Specify a filename for the 'client' mode.")
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
    log.debug("Connecting to Zookeeper using host param [{0}]".format(host))
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


def check_hdfs_compat():
    """Checks if we can connect to HDFS and it's a tested version"""
    # TODO: Collect version number and pass back that or error
    hdfs_compat_iter = run_shell_command(['hdfs', 'version'])
    result = False
    firstline = hdfs_compat_iter.next()   # Firstline should be version number if it works
    for vers in conf.COMPAT:
        if vers in firstline:
            result = True
    return result


def check_hdfs_for_file(target):
    """
    Checks if file requested for shredding actually exists on HDFS.
    Returns True if file is Found.
    Returns Error details if it is not found.
    """
    # TODO: Return canonical path from LS command rather than trusting user input
    log.debug("Checking validity of HDFS File target [{0}]".format(target))
    file_check_isDir = subprocess.call(['hdfs', 'dfs', '-test', '-d', target])
    log.debug("File Check isDir returned [{0}]".format(file_check_isDir))
    if file_check_isDir is 0:   # Returns 0 on success
        raise ValueError("Target [{0}] is a directory.".format(target))
    file_check_isFile = subprocess.call(['hdfs', 'dfs', '-test', '-e', target])
    log.debug("File Check isFile returned [{0}]".format(file_check_isFile))
    if file_check_isFile is not 0:    # Returns 0 on success
        raise ValueError("Target [{0}]: File not found.".format(target))
    else:
        return True


def get_fsck_output(target):
    """Runs HDFS FSCK on the HDFS File to get block location information for Linux shredder"""
    # fsck_out_iter = run_shell_command(['cat', 'sample-data.txt'])
    fsck_out_iter = run_shell_command(["hdfs", "fsck", target, "-files", "-blocks", "-locations"])
    log.debug("Fsck_out type is [{0}]".format(type(fsck_out_iter)))
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
    log.debug("FSCK parser output [{0}]".format(output))
    return output


def write_blocks_to_zk(zk_conn, data):
    """Write block to be deleted to zookeeper"""
    log.debug("ZK Writer passed blocklists for [{0}] Datanodes to shred".format(len(data)))
    for datanode_ip in data:
        log.debug("Processing blocklist for Datanode [{0}]".format(datanode_ip))
        zk_path_dn = conf.ZOOKEEPER['PATH'] + datanode_ip
        zk_conn.ensure_path(zk_path_dn)
        zk_conn.create(
            path=zk_path_dn + '/blocklist',
            value=compress(pickle.dumps(data[datanode_ip]))
        )
        zk_conn.create(
            path=zk_path_dn + '/status',
            value='file_not_deleted_blocklist_written'
        )
    log.debug("List of DN Blocklists written to ZK: [{0}]".format(zk_conn.get_children(conf.ZOOKEEPER['PATH'])))
    # TODO: Test ZK nodes are all created as expected
    # TODO: Handle existing ZK nodes
    return True


def delete_file_from_hdfs(target):
    """Uses HDFS Client to delete the file from HDFS
    Returns a Bool result"""
    return True


def get_datanode_ip():
    """Returns the IP of this Datanode"""
    # TODO: Write this function to return more than a placeholder
    return "127.0.0.1"


def read_blocks_from_zk(zk_conn, dn_id):
    """
    Read blocks to be deleted from Zookeeper
    Requires active ZooKeeper connection and the datanode-ID as it's IP as a string
    """
    # TODO: Check dn_id is valid
    log.debug("Attempting to read blocklist for Datanode [{0}]".format(dn_id))
    zk_path_dn = conf.ZOOKEEPER['PATH'] + dn_id
    dn_status = zk_conn.get(zk_path_dn + '/status')
    if dn_status[0] is 'file_not_deleted_blocklist_written':
        dn_node = zk_conn.get(zk_path_dn + '/blocklist')
        blocklist = pickle.loads(decompress(dn_node[0]))
        return blocklist
    else:
        raise ValueError("Blocklist Status for this DN is not as expected at [{0}]".format(zk_path_dn + '/status'))


def generate_shred_task_list(block_list):
    """Generate list of tasks of blocks to shred for this host"""
    # TODO: Write this function to return more than a placeholder
    output = {}
    return output


def shred_blocks(blocks):
    """Reliable shred process to ensure blocks are truly gone baby gone"""
    # TODO: Write this function to return more than a placeholder
    # TODO: Keep tracked of deleted block / files
    pass


def check_job_status(job_id):
    """Checks for job status in ZK and returns meaningful codes"""
    # TODO: return 'JobNotFound' if file is not listed in ZK
    pass


def write_job_zk(job_id):
    """Writes the filepath to ZK as a new delete/shred job"""
    pass


def main():
    ### Program setup
    log.info("shred.py called with args [{0}]").format(sys.argv[1:])
    # Get invoke parameters
    log.debug("Parsing args using Argparse module.")
    args = parse_args(sys.argv[1:])                                             # Test Written
    # Checking the config was pulled in
    log.debug("Checking for config parameters.")
    if not conf.VERSION:
        raise "Config from config.py not found, please check configuration file is available and try again."
    # Test necessary connections
    log.debug("Checking if we can find the HDFS client and HDFS instance to connect to.")
    if not check_hdfs_compat:                                                   # Test Written
        raise "Could not find HDFS, please check the HDFS client is installed and HDFS is available and try again."
    # Test for Zookeeper connectivity
    log.debug("Looking for ZooKeeper")
    zk_host = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
    zk = connect_zk(zk_host)                                  # Test Written
    ### End Program Setup

    if args.mode is 'client':
        log.debug("Detected that we're running in 'client' Mode")
        # Test if target file exists on HDFS
        log.debug("Checking if file exists in HDFS")
        file_exists = check_hdfs_for_file(args.file_to_shred)                   # Test Written
        if file_exists is not True:
            raise "File for shredding not found on HDFS: [{0}]".format(args.file_to_shred)
        else:
            # File exists, now check if job already exists
            status = check_job_status(args.file_to_shred)
            if status is "JobNotFound":
                # No job currently registered for this file, and file exists in HDFS
                # TODO: Add confirmation dialogue using the canonical filepath to check we are targeting correct file
                write_job_zk(args.file_to_shred)
            else:
                # TODO: Manage other status codes
                pass
            status = check_job_status(args.file_to_shred)
            if status is "JobFound":
                log.debug("Job created, exiting with success")
                print("Successfully created delete and shred job for file [{0}]".format(args.file_to_shred))
                exit(0)
            else:
                # TODO: Manage other status codes
                pass
    elif args.mode is 'worker':
        pass
        # Foot Datanode:
        # Check ZK for shred leader status, if leader keepalive not-ok, then run election
        # Check own Datanode for jobs
        # If job found, check in against job and begin prep:
        # find listed blk files and validate that there is a shred dir available on partition for cp
        # When validation completed, check in as ready to execute
        # When Krang commands, cp all local blk files to shred dir on local partition and update status
        # Check if Krang marks distributed job as successfully, if so, mark blks ready for shredding, else rollback - update status
        # Check Next job, else sleep till next check interval

        # If this Datanode is Krang, then:
        # Get all jobs from ZK into a worklist
        # If no jobs, update leader keepalive and sleep till next check interval
        # if Jobs, check all datanodes available else alert
        # For each job
        # Check status of job
        # if job ready for execute then get blocklist and write tasks for Foot Datanodes, update job status
        # wait for all datanodes to check in as in preparation - use 2x sleep timer as checkin timeout for job failure and reset
        # when all datanodes checked in as ready, prepare for distributed transaction
        # command all Foot to execute cp, if all return success (including self), then run HDFS delete command, else raise alert and rollback
        # if successful, update job status and wait for all Foot to confirm change to ready for shred status
        # if all Foot confirm final status update, mark job as ready for The Shredder.
    elif args.mode is 'shredder':
        pass
    else:
        raise "Bad operating mode [{0}] detected. Please consult program help and try again." \
            .format(args.mode)

### ver 0.0.2 process logic, with race condition flaw
    # if args.mode is 'file':
    #     log.debug("Detected that we're running in 'File' Mode")
    #     # Test if target file exists on HDFS
    #     log.debug("Checking if file exists in HDFS")
    #     file_exists = check_hdfs_for_file(args.file_to_shred)                   # Test Written
    #     if file_exists is not True:
    #         raise "File for shredding not found on HDFS: [{0}]".format(args.file_to_shred)
    #     # Get block information for the file we are to shred
    #     log.debug("Requesting FSCK information for file.")
    #     fsck_output = get_fsck_output(args.file_to_shred)                       # Test Written
    #     log.debug("Requesting parser to return clean dict of block files to shred")
    #     # Parse HDFS fsck output into a dictionary of datanodes with lists of block IDs
    #     blocks_dict_out = parse_blocks_from_fsck(fsck_output)                   # Test Written
    #     # Store blocks to be shredded into Zookeeper
    #     log.debug("Writing datanodes and blocks information to ZooKeeper for shredding workers")
    #     blocklist_status = write_blocks_to_zk(zk, blocks_dict_out)                                 # Test Written
    #     if blocklist_status is True:
    #         del_status = delete_file_from_hdfs(args.file_to_shred)
    #         if del_status is True:
    #             log.info("File [{0}] loaded for Shredding and deleted from HDFS")
    #             exit(0)
    #     else:
    #         raise StandardError("Failure to store HDFS file Blocklist to ZooKeeper for shredding, will not delete file.")
    # elif args.mode is 'blocks':
    #     log.debug("Detected that we're running in Block Shredding worker mode")
    #     # Get my IP
    #     log.debug("Determinging this DataNode's IP")
    #     dn_id = get_datanode_ip()                                               # TODO: Write Test
    #     # Get current blocks list from zk
    #     log.debug("Getting list of block shredding tasks for this Datanode")
    #     block_list_in = read_blocks_from_zk(zk, dn_id)                          # TODO: Write Test
    #     # Parse Block Dict into task list
    #     log.debug("Parsing block list into list of shredding tasks")
    #     shred_task_iter = generate_shred_task_list(block_list_in)               # TODO: Write Test
    #     # Execute Shred Processor
    #     log.debug("Requesting shredding task execution")
    #     shred_blocks(shred_task_iter)                                           # TODO: Write Test
    # else:
    #     raise "Bad operating mode [{0}] detected. Please retry by specifying mode of either 'file' or 'blocks'." \
    #         .format(args.mode)
### End ver 0.0.2 process logic

if __name__ == "__main__":
    main()

