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
from socket import gethostname, gethostbyname
from os.path import join as ospathjoin
from os.path import dirname, abspath
from kazoo.client import KazooClient
from datetime import timedelta as dttd
from hdfs import Config, HdfsError
from json import dumps, loads

from config import conf

# Set to True to enhance logging when working in a development environment
test_mode = True

log = logging.getLogger('apriloneil')
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

# Begin Function definitions


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


def connect_zk():
    """create connection to ZooKeeper"""
    # Construct Host from Config
    host = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
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
    log.debug("Attempting to instantiate HDFS client")
    # TODO: Write try/catch for connection errors and states
    global hdfs
    try:
        hdfs = Config("./config/hdfscli.cfg").get_client()
    except HdfsError:
        try:
            hdfs = Config(dirname(__file__) + "/config/hdfscli.cfg").get_client()
        except HdfsError:
            log.error("Couldn't find HDFS config file")
            exit(1)
    if hdfs:
        return hdfs
    else:
        raise StandardError("Unable to connect to HDFS, please check your configuration and retry")


def run_shell_command(command):
    """Read output of shell command - line by line"""
    log.debug("Running Shell command [{0}]".format(command))
    # http://stackoverflow.com/a/13135985
    p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
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


def set_status(job_id, component, status):
    """Abstracts setting a given status for a given job, job subcomponent, and status message"""
    # determine file to be written
    if component == "master":
        # The master component always updates the state of the job in the master job list
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job_id)
    elif component == "data":
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, "status")
    else:
        # otherwise we update the file named for that component in the subdir for the job in the general store
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, component, "status")
    log.debug("Setting status of component [{0}] at path [{1}] to [{2}]".format(component, file_path, status))
    if file_path is not None:
        hdfs.write(file_path, status, overwrite=True)
    else:
        raise ValueError("File Path to set job status not set.")


def init_new_job():
    """and generates job management files and dirs"""
    # Generate a guid for a job ID
    job_id = str(uuid4())
    status = 'stage1init'
    component = 'master'
    log.debug("Generated uuid [{0}] for job identification".format(job_id))
    # Create directory named with a guid, create an initial status file in it for tracking this job
    set_status(job_id, component, status)
    # Create subdir for data
    component = 'data'
    set_status(job_id, component, status)
    # return status and job guid
    return job_id, status


def ingest_targets(job_id, target):
    """Moves file from initial location to shred worker folder on HDFS"""
    # Update status'
    status = "stage1ingest"
    component = "master"
    set_status(job_id, component, status)
    component = "data"
    set_status(job_id, component, status)
    # Move all files to the data directory
    path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, 'data')
    # Using the HDFS module's rename function to move the target files
    log.debug("Moving target file [{0}] to shredder holding pen [{1}]".format(target, path))
    # We need to ensure the directory is created, or the rename command will dump the data into the file
    hdfs.makedirs(path)
    hdfs.rename(target, path)
    # update status
    status = "stage1ingestComplete"
    set_status(job_id, component, status)
    return job_id, status


def finalise_client(job_id, target):
    component = "master"
    status = "stage1complete"
    set_status(job_id, component, status)
    log.debug("Job [{0}] prepared, exiting with success".format(job_id))
    print("Successfully created Secure Delete job for file [{0}]".format(target))
    return True


def get_worker_identity():
    """Determines a unique identity string to use for this worker"""
    # TODO: Implement something more robust than a simple IP lookup!!!
    # Doing a brutal match by hostname or IP is not robust enough!
    # worker_id = gethostname()
    worker_id = gethostbyname(gethostname())
    return worker_id


def get_jobs_by_status(target_status):
    """Checks for the existance of new worker jobs and returns a list of them if they exist"""
    worker_job_list = []
    # check if dir exists as worker my load before client is ever used
    job_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs")
    job_dir_exists = hdfs.content(job_path, strict=False)
    if job_dir_exists is not None:
        # if job dir exists, get listing and any files
        dirlist = hdfs.list(job_path, status=True)
        for item in dirlist:
            if item[1]['type'] == 'FILE':
                with hdfs.read(ospathjoin(job_path, item[0])) as reader:
                    job_status = reader.read()
                # if file contains the completion status for stage1, put it in worker list
                if job_status == target_status:
                    worker_job_list.append(item[0])
    return worker_job_list


def get_target_by_jobid(job_id):
    """Gets paths of target files ingested into this jobs data store directory
    returns list of absolute paths to target files on HDFS"""
    hdfs_file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, "data")
    log.debug("getting list of files at path [{0}]".format(hdfs_file_path))
    # hdfs.list returns a list of file names in a directory
    hdfs_file_list = hdfs.list(hdfs_file_path)
    out = []
    for file in hdfs_file_list:
        # TODO: Check if HDFS always uses / as path separator on Win or Linux etc.
        out.append(hdfs_file_path + '/' + file)
    return out


def get_fsck_output(target):
    """Runs HDFS FSCK on the HDFS File to get block location information for Linux shredder"""
    # fsck_out_iter = run_shell_command(['cat', 'sample-data.txt'])
    fsck_out_iter = run_shell_command(["hdfs", "fsck", target, "-files", "-blocks", "-locations"])
    log.debug("Fsck_out type is [{0}]".format(type(fsck_out_iter)))
    return fsck_out_iter


def parse_blocks_from_fsck(raw_fsck):
    """
    Separate parser for FSCK output to make maintenance easier
    Takes an iterator of the hdfs fsck output
    Returns a dict keyed by IP of each datanode with a list of blk ids
    example: {'172.16.0.80': ['blk_1073839025'], '172.16.0.40': ['blk_1073839025'], '172.16.0.50': ['blk_1073839025']}
    """
    output = {}
    while True:
        try:
            current_line = raw_fsck.next()
            if current_line[0].isdigit():
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


def prepare_blocklists(job_id):
    """Attempts to take leadership for job preparation and creates the block-file lists for each datanode worker"""
    # attempt to kazoo lease new guid node for sleep period minutes
    log.debug("Preparing Blocklists for job [{0}]".format(job_id))
    log.debug("Attempting to get lease as leader for job")
    lease = zk.NonBlockingLease(
        path=conf.ZOOKEEPER['PATH'] + job_id,
        duration=dttd(minutes=conf.WORKER_SLEEP),
        identifier="Worker [{0}] preparing blocklists for job [{1}]".format(get_worker_identity(), job_id)
    )
    # http://kazoo.readthedocs.io/en/latest/api/recipe/lease.html
    # if not get lease, return pipped status
    if not lease:
        log.debug("Beaten to leasehold by another worker")
        return "pipped"
    else:
        # TODO: Add Lease management, there's probably a with... function here somewhere for it
        log.debug("Got lease as leader on job, updating job status")
        # update job status to stage2prepareblocklist
        status = "stage2prepareBlocklist"
        component = "master"
        set_status(job_id, component, status)
        # get job target ( returns a list )
        targets = get_target_by_jobid(job_id)
        log.debug("Got target file(s) [{0}] for job".format(targets))
        # get fsck data for targets
        blocklists = {}
        for target in targets:
            fsck_data = get_fsck_output(target)
            # parse fsck data for blocklists
            blocklists.update(parse_blocks_from_fsck(fsck_data))
        log.debug("Parsed FSCK output for target files: [{0}]".format(blocklists))
        # match fsck output to worker_ids
            # block IDs for workers are currently the IP of the datanode, which matches our worker_id in the utility
            # Therefore no current need to do a match between the fsck output and the local worker ID
        # write a per-DN file to hdfs job subdir for other workers to read
        target_workers = blocklists.keys()
        log.debug("Datanode list for these blockfiles is: [{0}]".format(target_workers))
        for this_worker in target_workers:
            this_worklist = {}
            for blockfile in blocklists[this_worker]:
                this_worklist[blockfile] = "new"
            workfile_content = dumps(this_worklist)
            file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, this_worker)
            log.debug("Writing [{0}] to workfile [{1}] for Datanode [{2}]"
                      .format(workfile_content, file_path, this_worker))
            hdfs.write(file_path, workfile_content, overwrite=True)
        # update job status to stage2copyblocks
        log.debug("Completed leader tasks for blocklist preparation, updating status and returning from function")
        status = "stage2copyblocks"
        set_status(job_id, component, status)
        # TODO: Look for a method to explicitly release the lease when done
        # apparently there's no release lease command in this recipe, so it'll just timeout?
        # return success status
        return "success"

# End Function definitions


def main():
    # Program setup
    log.info("shred.py called with args [{0}]").format(sys.argv[1:])
    # Get invoke parameters
    log.debug("Parsing args using Argparse module.")
    args = parse_args(sys.argv[1:])                                             # Test Written
    # Checking the config was pulled in
    # TODO: Move to full configuration file validation function
    log.debug("Checking for config parameters.")
    if not conf.VERSION:
        raise StandardError(
            "Version number in config.py not found, please check configuration file is available and try again."
        )
    # Test necessary connections
    connect_hdfs()
    # Check directories etc. are setup
    hdfs.makedirs(conf.HDFS_SHRED_PATH)
    # TODO: Further Application setup tests
    # End Program Setup

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
            job_id, job_status = init_new_job()
            if 'stage1init' not in job_status:
                raise "Could not create job for file: [{0}]".format(target)
            else:
                log.info("Created job id [{0}] for target [{1}]. Current status: [{2}]".format(
                    job_id, target, job_status
                ))
                job_id, status = ingest_targets(job_id, target)
                if status != "stage1ingestComplete":
                    raise StandardError(
                        "Ingestion failed for file [{0}] for job [{1}], please see log and status files for details"
                        .format(target, job_id)
                    )
                else:
                    ready_for_exit = finalise_client(job_id, target)
                    if ready_for_exit:
                        exit(0)
                    else:
                        raise StandardError("Unexpected program exit status, please refer to logs")
    elif args.mode is 'worker':
        # Determine identity
        worker_id = get_worker_identity()
        # wake from sleep mode
        log.info("Worker [{0}] activating".format(worker_id))
        # Establishing HDFS connection
        connect_hdfs()
        # check if there are jobs to be done
        worklist = get_jobs_by_status('stage1complete')
        # if no new do next section
        if len(worklist) > 0:
            log.info("New jobs found: [{0}]".format(worklist))
            # connect to ZK
            zk = connect_zk()
            # for each job in list
            for job_id in worklist:
                # if no guid node indicating the lease is available
                if zk.exists(conf.ZOOKEEPER['PATH'] + job_id):
                    result = prepare_blocklists(job_id)
                    if result == "success":
                        log.info("Worker [{0}] successfully prepared blocklists for job [{0}]"
                                 .format(worker_id, job_id))
                    elif result == "pipped":
                        log.info("Attempted to run blocklist preparation for job [{0}] but was beaten to it by"
                                 "another worker, are the workers on a concurrent schedule?".format(job_id))
                    else:
                        raise StandardError("Unexpected return status from blocklist preparation task")
        else:
            log.info("No new jobs found")
            pass
        # Begin non-leader functionality for Worker
        worklist = get_jobs_by_status('stage2copyblocks')
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
        # Move job from incomplete to completed
    else:
        raise "Bad operating mode [{0}] detected. Please consult program help and try again.".format(args.mode)


if __name__ == "__main__":
    main()
