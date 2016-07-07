#!/usr/bin/python
# coding: utf-8

"""
Proof of Concept for Secure Delete on Hadoop; to shred files deleted from HDFS for audit compliance.
See https://github.com/Chaffleson/hdfs-shred
"""

import logging
import logging.handlers
from syslog_rfc5424_formatter import RFC5424Formatter
import re
import subprocess
import sys
import argparse
from uuid import uuid4
from os.path import join as ospathjoin
from os.path import dirname, abspath
from kazoo.client import KazooClient
from hdfs import Config

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

# Global Handles
zk = None
hdfs = None

### Begin Function definitions


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
    if result.mode is 'client' and result.filename is None:
        log.error("Argparse found a bad arg combination, posting info and quitting")
        parser.error("--mode 'client' requires a filename to register for shredding.")
    if result.mode in ['worker', 'shredder'] and result.filename:
        log.error("Argparse found a bad arg combination, posting info and quitting")
        parser.error("--mode 'worker' or 'shredder' cannot be used to register a new filename for shredding."
                     " Please try '--mode client' instead.")
    log.debug("Argparsing complete, returning args to main function")
    return result


def connect_zk(host):
    """create connection to ZooKeeper"""
    log.debug("Connecting to Zookeeper using host param [{0}]".format(host))
    global zk
    zk = KazooClient(hosts=host)
    zk.start()
    if zk.state is 'CONNECTED':
        log.debug("Asserting Zookeeper connection is live to main function")
        return zk
    else:
        raise "Could not connect to ZooKeeper with configuration string [{0}], resulting connection state was [{1}]"\
            .format(host, zk.state)


def connect_hdfs():
    """Uses HDFS client module to connect to HDFS
    returns handle object"""
    log.debug("Instatiating HDFS client")
    # TODO: Write try/catch for connection errors and states
    global hdfs
    hdfs = Config(dirname(__file__) + "/config/hdfscli.cfg").get_client()
    if hdfs:
        return hdfs
    else:
        return False


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


def check_hdfs_for_target(target):
    """
    Checks if file requested for shredding actually exists on HDFS.
    Returns True if file is Found.
    Returns Error details if it is not found.
    """
    # TODO: Return canonical path from LS command rather than trusting user input
    log.debug("Checking validity of HDFS target [{0}]".format(target))
    target_details = hdfs.status(target, strict=False)
    log.debug("HDFS status is: [{0}]".format(target_details))
    if target_details is not None and target_details['type'] == u'FILE':
        return True
    else:
        return False


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


def set_status(job_id, component, status):
    """Abstracts setting a given status for a given job, job subcomponent, and status message"""
    # determine file to be written
    path = ""
    if component == "master":
        path = ospathjoin(conf.HDFS_SHRED_PATH, job_id)
    else:
        path = ospathjoin(conf.HDFS_SHRED_PATH, job_id, component) 
    # Ensure directory structure
    hdfs.makedirs(path)
    # Write status file
    hdfs.write(ospathjoin(path, "status"), status, overwrite=True)


def prepare_job(target):
    """and generates job management files and dirs"""
    status = 'stage1init'
    component = 'master'
    # Generate a guid for a job ID
    job_id = str(uuid4())
    log.debug("Generated uuid [{0}] for job identification".format(job_id))
    # Create directory named with a guid, create an initial status file in it
    set_status(job_id, component, status)
    # Create subdir for data
    component = 'data'
    set_status(job_id, component, status)
    # return status and job guid
    return (job_id, status)


def ingest_targets(job_id, target):
    """Moves file from initial location to shred worker folder on HDFS"""
    # Update status'
    status = "stage1ingest"
    component = "master"
    set_status(job_id, component, status)
    component = "data"
    set_status(job_id, component, status)
    # Move all files to the data directory
    path = ospathjoin(conf.HDFS_SHRED_PATH, job_id, 'data')
    # Using the HDFS module's rename function to move the target files
    log.debug("Moving target file [{0}] to shredder holding pen [{1}]".format(target, path))
    hdfs.rename(target, path)
    # update status
    status = "stage1ingestComplete"
    set_status(job_id, component, status)
    return (job_id, status)

### End Function definitions


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
    hdfs_connected = connect_hdfs()
    if not hdfs_connected:
        raise "Unable to connect to HDFS, please check your configuration and retry"
    # Check directories etc. are setup
    hdfs.makedirs(conf.HDFS_SHRED_PATH)
    # TODO: Further Application setup tests
    ### End Program Setup

    if args.mode is 'client':
        log.debug("Detected that we're running in 'client' Mode")
        # forcing target to be absolute pathed for safety
        target = abspath(args.file_to_shred)
        log.debug("Checking if file exists in HDFS")
        target_exists = check_hdfs_for_target(target)
        if target_exists is not True:
            raise "Submitted File not found on HDFS: [{0}]".format(target)
        else:
            # By using the client to move the file to the shred location we validate that the user has permissions
            # to call for the delete and shred
            job_id, job_status = prepare_job(target)
            if 'stage1init' not in job_status:
                raise "Could not create job for file: [{0}]".format(target)
            else:
                log.info("Created job id [{0}] for target [{1}]. Current status: [{2}]".format(
                    job_id, target, job_status
                ))
                job_id, status = ingest_targets(job_id, target)
                if status != "stage1ingestComplete":
                    raise "Ingestion failed for target file [{0}] for job [{1}], please see log and status files for details".format(target, job_id)
                else:
                    component = "master"
                    status = "stage1complete"
                    set_status(job_id, component, status)
                    log.debug("Job [{0}] prepared, exiting with success".format(job_id))
                    print("Successfully created Secure Delete job for file [{0}]".format(target))
                    exit(0)
    elif args.mode is 'worker':
        pass
        # wake from sleep mode
        # check if there are new files in HDFS:/.shred indicating new jobs to be done
        # if no new jobs, sleep
        # else, check files for status
        # if status is stage1complete, connect to ZK
        zk_host = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
        zk = connect_zk(zk_host)                                                # Test Written
        # if no guid node, attempt to kazoo lease new guid node for 2x sleep period minutes
        # http://kazoo.readthedocs.io/en/latest/api/recipe/lease.html
        # if not get lease, pass, else:
        # update job status to stage2prepareblocklist
        # parse fsck for blocklist, write to hdfs job subdir for other workers to read 
        # update job status to stage2copyblocks
        # release lease
        #
        # Foreach job in subdirs
        # if status is stage2copyblocks
        # parse blocklist for job
        # if blocks for this DN
        # update DN status to Stage2running
        # create tasklist file under DN subdir in job
        # foreach blockfile in job:
        # update tasklist file to finding
        # find blockfile on ext4
        # update tasklist file to copying
        # create hardlink to .shred dir on same partition
        # update tasklist file to copied
        # When all blocks finished, update DN status to Stage2complete
        #
        # if all blocks copied, attempt lease of guid for 2x sleep period minutes, else sleep for 1x period minutes
        # if lease:
        # Update DN status to Stage2leaderactive
        # periodically check status file of each DN against DNs in blocklist
        # if DN not working within 2x sleep period minutes, alert
        # if DN all working but not finished, update Dn status to Stage2leaderwait, short sleep
        # if all DN report finished cp, update job status stage2readyfordelete
        # run hdfs delete files -skiptrash, update status to stage2filesdeleted
        # update central status as stage2complete, update Dn status as Stage2complete
        # release lease, shutdown
    elif args.mode is 'shredder':
        pass
        # wake on schedule
        # Foreach job in subdir:
        # if Stage2Complete
        # Get DN tasklist for job
        # Set DN status to Shredding for job
        # Foreach blockfile:
        # set status to shredding in tasklist
        # run shred
        # set status to shredded in tasklist
        # when job complete, set DN status to Stage3complete
    else:
        raise "Bad operating mode [{0}] detected. Please consult program help and try again.".format(args.mode)


if __name__ == "__main__":
    main()

