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
from time import sleep
from json import dumps, loads
from datetime import timedelta as dttd
from uuid import uuid4
from socket import gethostname, gethostbyname
from os.path import join as ospathjoin
from os.path import dirname, realpath, ismount, exists, split
from os import link, makedirs
from kazoo.client import KazooClient, KazooState
from hdfs import Config, HdfsError

from config import conf

# TODO: Build into an Ambari agent to handle distribution and monitoring perhaps?
# Apologies to maintainers; I could not resist a few TMNT 1987 references for an application involving The Shredder...

# ###################          Logging            ##########################

try:
    log = logging.getLogger(__file__)
except NameError:
    log = logging.getLogger('apriloneil')
log_level = logging.getLevelName(conf.LOG_LEVEL)
log.setLevel(log_level)
handler = logging.handlers.SysLogHandler(address='/dev/log')
handler.setFormatter(RFC5424Formatter())
log.addHandler(handler)
if conf.TEST_MODE:
    con_handler = logging.StreamHandler()
    log.addHandler(con_handler)

# ###################     Global conection handles     ##########################
zk = None
hdfs = None

# ###################          Begin Function definitions           ##########################


def parse_user_args(args):
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
    """create global connection handle to ZooKeeper"""
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


# @zk.add_listener
# def zk_lost_emergency_exit(state):
#     # reminder:
#     # http://www.slideshare.net/OReillyOSCON/distributed-coordination-with-python
#     if state != KazooState.CONNECTED:
#         logging.critical("ZooKeeper connection dropped, closing.")
#         sys.exit(1)


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
    # TODO: This is dumb, if it's global it should be tidier even for tests
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


def init_new_job():
    """and generates job management files and dirs"""
    # Generate a guid for a job ID
    job_id = str(uuid4())
    status = 'stage1init'
    log.debug("Generated uuid [{0}] for job identification".format(job_id))
    # Create directory named with a guid, create an initial status file in it for tracking this job
    set_hdfs_file(job_id, 'master', status)
    # Create subdir for data
    set_hdfs_file(job_id, 'data_status', status)
    # return status and job guid
    return job_id, status


def ingest_targets(job_id, source_file):
    """Moves file from initial location to shred worker folder on HDFS"""
    # Update status'
    # TODO: Update sequence to handle list of input files instead of single as a string
    set_hdfs_file(job_id, "master", "stage1ingest")
    set_hdfs_file(job_id, "data_status", "initialisingIngest")
    # Move all files to the data directory
    target_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, 'data')
    # Using the HDFS module's rename function to move the target files
    log.debug("Moving target file [{0}] to shredder holding pen [{1}]".format(source_file, target_path))
    # We need to ensure the directory is created, or the rename command will dump the data into the file
    hdfs.makedirs(target_path)
    # TODO: Handle failure results from rename command, bad permissions etc.
    hdfs.rename(source_file, target_path)
    # Sanity checks
    # TODO: Write more sanity checks for ingest process
    source_path, source_filename = split(source_file)
    # set resulting ingested filename for later ease of use
    expected_target_realpath = ospathjoin(target_path, source_filename)
    set_hdfs_file(job_id, "data_filelist", expected_target_realpath)
    # update status
    set_hdfs_file(job_id, "master", "stage1ingestComplete")
    return job_id, "stage1ingestComplete"


def finalise_client(job_id, target):
    component = "master"
    status = "stage1complete"
    set_hdfs_file(job_id, component, status)
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
    job_dir_exists = None
    try:
        job_dir_exists = hdfs.content(job_path, strict=False)
    except AttributeError:
        log.error("HDFS Client not connected")
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


def worker_leader_delete_hdfs_targets(job_id):
    connect_zk()
    worker_id = get_worker_identity()
    result = None
    # attempt lease of leader for 1x wait period minutes, else skip
    # we use a non-blocking lease for this job, so that another worker can move onto a different job
    lease = zk.NonBlockingLease(
        path=conf.ZOOKEEPER['PATH'] + job_id,
        duration=dttd(minutes=conf.WORKER_WAIT),
        identifier="Worker [{0}] preparing HDFS delete for job [{1}]".format(worker_id(), job_id)
    )
    # if we get the lease for this job
    if lease:
        # While we still have the lease for this job
        log.info("Worker [{0}] gained leader lease for job [{1}]"
                 .format(worker_id, job_id))
        while lease:
            # while we haven't achieved a result from the workflow
            while result is None:
                # if we haven't disconnected from zookeeper!
                if zk.state != KazooState.CONNECTED:
                    log.error("Lost connection to ZooKeeper during leader actions, expiring leader activity")
                    result = "disconnected"
                # Update the state of the distribtued workers from their status files on HDFS
                log.debug("Worker [{0}] leading job [{1}] updating state map of distributed workers' linking tasks"
                          .format(worker_id, job_id))
                workers_state = get_workers_state_by_job(job_id)
                # if all DN report finished their linking for this job
                if workers_state == "finished":
                    log.debug("Worker [{0}] leading job [{1}] all worker linking tasks reporting as completed"
                              .format(worker_id, job_id))
                    # Ready to continue, update master job status stage2readyfordelete
                    # This is the master gateway status out of the distributed file linking activity
                    # TODO: Insert final sanity check before running delete of file from HDFS
                    set_hdfs_file(job_id, "master", "stage2readyForDelete")
                    # get file(s) to be deleted from list generated at ingest
                    deletion_target = get_hdfs_file(job_id, "data_filelist")
                    # TODO: Redo this as a list process for multiple file submission
                    # run hdfs delete files -skiptrash
                    # Can't use the hdfscli delete command as it doesn't leverage the skiptrash flag
                    log.debug("Worker [{0}] leading job [{1}] deleting target file [{2}] from hdfs"
                              .format(worker_id, job_id, deletion_target))
                    delete_cmd = ['hdfs', 'dfs', '-rm', '-skipTrash', deletion_target]
                    cmd_output = next(run_shell_command(delete_cmd))
                    if "Deleted" in cmd_output:
                        # update data status to stage2filesdeleted
                        set_hdfs_file(job_id, "data_status", "stage2filesdeleted")
                        # update master status as stage2complete
                        set_hdfs_file(job_id, "master", "stage2complete")
                        # release lease, shutdown
                        result = "success"
                    else:
                        log.error("Deletion of file from HDFS returned unexpected result of [{0}], bailing"
                                  .format(cmd_output))
                        result = "failed"
                elif workers_state == "failures":
                    # raise alerts and instruct log file review
                    log.warning("Failures reported by workers for job [{0}], bailing...".format(job_id))
                    result = "failed"
                elif workers_state == "nonStarters":
                    # log that we are still waiting for some workers to start
                    log.info("Waiting for workers to start for job [{0}], sleeping for [{1}] mins"
                             .format(job_id, conf.WORKER_WAIT))
                    sleep(60 * conf.WORKER_WAIT)
                elif workers_state == "started":
                    # log that workers are in progress, sleep
                    log.info("Workers for job [{0}] in progress without reported failures, sleeping for [{1}] mins"
                             .format(job_id, conf.WORKER_WAIT))
                    sleep(60 * conf.WORKER_WAIT)
                else:
                    # shouldn't be here
                    log.warning("Unexpected state [{0}] returned from workers for job [{1}], bailing"
                                .format(workers_state, job_id))
                    result = "failed"
        else:
            # We should only be here if the lease expired
            result = "expired"
    else:
        # Did not obtain lease, time to move onto another job
        result = "notLeader"
    if result is not None:
        # Looks like the we completed this task in some manner without faulting, returning the result
        return result
    else:
        return "failed"


def get_workers_state_by_job(job_id):
    # check status file of each DN against DNs in blocklist
    worker_list = get_hdfs_file(job_id, "worker_list")
    worker_state_map = {
        "notInit": [],
        "notStarted": [],
        "started": [],
        "failed": [],
        "finished": [],
        "unexpected": []
    }
    result = None
    for this_worker in worker_list:
        this_worker_state = get_hdfs_file(job_id, "worker_" + this_worker + "_status")
        if this_worker_state is None:
            worker_state_map["notInit"].append(this_worker)
        elif "stage1" in this_worker_state:
            worker_state_map["notStarted"].append(this_worker)
        elif this_worker_state in ['stage2linkingFailed']:
            worker_state_map["failed"].append(this_worker)
        elif this_worker_state in ['stage2linkingComplete', 'stage2linkingNotRequired']:
            worker_state_map["finished"].append(this_worker)
        elif "stage2" in this_worker_state:
            worker_state_map["started"].append(this_worker)
        else:
            worker_state_map["unexpected"].append(this_worker)
    log.info("Distributed worker state for job [{0}] is: {1}".format(job_id, dumps(worker_state_map)))
    if len(worker_state_map["finished"]) == len(worker_list):
        result = "finished"
    elif len(worker_state_map["failed"]) > 0 or len(worker_state_map["unexpected"]) > 0:
        result = "failures"
    elif len(worker_state_map["notInit"]) > 0 or len(worker_state_map["notStarted"]) > 0:
        result = "nonStarters"
    else:
        result = "started"
    # TODO: Do stuff to validate count and expected names of workers are all correct
    # We want the unique list of validated states, not the detail
    return result


def worker_leader_prepare_blocklists(job_id):
    """Attempts to take leadership for job preparation and creates the block-file lists for each datanode worker"""
    # attempt to kazoo lease new guid node for wait period minutes
    log.debug("Preparing Blocklists for job [{0}]".format(job_id))
    log.debug("Attempting to get lease as leader for job")
    lease = zk.NonBlockingLease(
        path=conf.ZOOKEEPER['PATH'] + job_id,
        duration=dttd(minutes=conf.WORKER_WAIT),
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
        set_hdfs_file(job_id, component, status)
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
            set_hdfs_file(job_id, "worker_" + this_worker + "_blockfilelist", dumps(this_worklist))
        # write list of workers to file for later retrieval
        set_hdfs_file(job_id, "worker_list", dumps(target_workers))
        log.debug("Completed leader tasks for blocklist preparation, returning from function")
        # TODO: Look for a method to explicitly release the lease when done
        # apparently there's no release lease command in this recipe, so it'll just timeout?
        # return success status
        return "success"


# http://stackoverflow.com/a/4453715
def find_mount_point(path):
    # 
    path = realpath(path)
    while not ismount(path):
        path = dirname(path)
    return path


def set_hdfs_file(job_id, component, content):
    """Abstracts setting a given status for a given job, job subcomponent, and status message"""
    # determine file to be written
    if component == "master":
        # The master component always updates the state of the job in the master job list
        # the Master job list is in a separate directory to make it easier to manually deactivate jobs without removing 
        # their data or history from the store folder
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job_id)
    elif 'worker' in component or 'data' in component:
        # Note to maintainers, this is deliberately checking if the strings are in the component name
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, component)
    else:
        raise StandardError("Function set_hdfs_file was passed an unrecognised component name")
    if file_path is not None:
        log.debug("Setting status of component [{0}] at path [{1}] to [{2}]".format(component, file_path, content))
        hdfs.write(file_path, content, overwrite=True)
    else:
        raise ValueError("File Path not set for function set_hdfs_file with component [{0}] and content [{1}]"
                         .format(component, content))


def get_hdfs_file(job_id, component):
    file_content = None
    if component == "master":
        # The master component always updates the state of the job in the master job list
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job_id)
    elif 'worker' in component or 'data' in component:
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, component)
    else:
        raise ValueError("Invalid option passed to function get_hdfs_file")
    try:
        with hdfs.read(file_path) as reader:
            # expecting all content by this program to be serialised as json
            file_content = loads(reader.read())
    except HdfsError:
        # if the file does not exist or is empty, we will return None anyway
        pass
    return file_content


# ###################          End Function definitions           ##########################

# ###################          Begin Workflow definitions           ##########################


def init_program(passed_args):
    log.info("shred.py called with args [{0}]").format(sys.argv[1:])
    # Get invoke parameters
    log.debug("Parsing args using Argparse module.")
    parsed_args = parse_user_args(sys.argv[1:])  # Test Written
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
    return parsed_args


def client_workflow(passed_args):
    log.debug("Detected that we're running in 'client' Mode")
    # forcing target to be absolute pathed for safety
    target = realpath(passed_args.file_to_shred)
    # TODO: Validate passed file target(s) further, for ex trailing slashes or actually a directory
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


def worker_workflow(passed_args):
    # Set up worker environment
    # Determine identity of this Datanode as the worker_id
    worker_id = get_worker_identity()
    # log wake from sleep mode
    log.info("Worker [{0}] activating".format(worker_id))
    # Establishing HDFS connection
    connect_hdfs()
    # Begin leader workflow for init'ing worker tasks
    #
    # Check for open jobs and seek leader lease in ZK, do blocklists if so
    worklist = get_jobs_by_status('stage1complete')
    # if no new do next section
    if len(worklist) > 0:
        log.info("New jobs found: [{0}]".format(worklist))
        # connect to ZK
        zk = connect_zk()
        # for each job in list
        for job_id in worklist:
            set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2activated")
            # Attempt to get lease and run leader init tasks
            result = worker_leader_prepare_blocklists(job_id)
            if result == "success":
                log.info("Worker [{0}] successfully prepared blocklists for job [{0}]"
                         .format(worker_id, job_id))
                # set master job status for distributed workers to wake up to
                set_hdfs_file(job_id, "master", "stage2copyblocks")
            elif result == "pipped":
                set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2waiting")
                log.info("Attempted to run blocklist preparation for job [{0}] but was beaten to it by"
                         "another worker, are the workers on a concurrent schedule?".format(job_id))
                # we don't touch the master status as the worker with the lease owns it
            else:
                raise StandardError("Unexpected return status from blocklist preparation task")
    else:
        log.info("No new jobs found, proceeding to look for existing jobs")
        pass
    # Begin non-leader, distributed functionality for Worker
    #
    # This status acts as a gate to running the next stage of the process
    joblist = get_jobs_by_status('stage2copyblocks')
    # If there are jobs ready for further action...
    if len(joblist) > 0:
        log.info("Worker [{0}] found active jobs in status [stage2copyblocks]".format(worker_id))
        # Parse jobs for files referencing this worker, and collect the blockfiles to be linked
        for job_id in joblist:
            tasklist = {}
            # Set status for this worker against this job
            set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2checkingForBlockList")
            this_job_blocklist = get_hdfs_file(job_id, ("worker_" + worker_id + "_blockfilelist"))
            if this_job_blocklist is not None:
                tasklist = this_job_blocklist
                set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2blocklistFound")
            else:
                set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2noBlocklistFound")
            # Tasklist should now be either empty, or a dict keyed by block file id
            if len(tasklist) > 0:
                shred_dirs = []
                set_hdfs_file(job_id, "worker_" + worker_id + "_status", 'stage2processingLinking')
                # foreach blockfile in active job:
                for this_block_file in tasklist:
                    if tasklist[this_block_file] == "new":
                        log.debug("Doing OS Find for blockfile [{0}] on worker [{1}] for job [{2}]"
                                  .format(this_block_file, worker_id, job_id))
                        tasklist[this_block_file] = "finding"
                        # TODO: Set search root to HDFS FS root from configs
                        find_root = "/"
                        find_cmd = ["find", find_root, "-name", this_block_file]
                        block_find_iter = run_shell_command(find_cmd)
                        # TODO: Handle file not found and other errors
                        found_files = []
                        for file in block_find_iter:
                            found_files.append(file.rstrip('\n'))
                        # TODO: Handle multiple files found
                        if len(found_files) == 1:
                            this_file = found_files[0]
                            log.debug("Found blockfile [{0}] at loc [{1}]".format(this_block_file, this_file))
                            tasklist[this_block_file] = "linking"
                            # Find the mount point for this file
                            this_file_part = find_mount_point(this_file)
                            # ensure we have a .shred dir available to link into in this mount point
                            this_part_shred_dir = ospathjoin(this_file_part, conf.LINUXFS_SHRED_PATH, job_id)
                            if not exists(this_part_shred_dir):
                                makedirs(this_part_shred_dir)
                            # Generate expected path + file combo
                            linked_filepath = ospathjoin(this_part_shred_dir, this_block_file)
                            # Link the blkfile to it
                            link(this_file, linked_filepath)
                            # Record the destination ready for shredding later
                            shred_dirs.append(linked_filepath)
                            log.debug("Linked blockfile [{0}] at loc [{1}] to shred loc at [{2}]"
                                      .format(this_block_file, this_file, this_part_shred_dir))
                            tasklist[this_block_file] = "linked"
                        else:
                            log.error("Found unexpected number of instances of blockfile on the local OS filesystem.")
                    elif tasklist[this_block_file] == "linked":
                        log.info("blockfile [{0}] already linked".format(this_block_file))
                    else:
                        log.warning("blockfile [{0}] in unexpected state [{1}]"
                                    .format(this_block_file, tasklist[this_block_file]))
                # update the resulting status of the blockfiles to the job blocklist regardless of outcome
                set_hdfs_file(job_id, "worker_" + worker_id + "_blockfilelist", dumps(tasklist))
                # write out the shred paths for this job for later reference
                set_hdfs_file(job_id, "worker_" + worker_id + "_shredfilelist", dumps(shred_dirs))
                # sanity test if linking is completed successfully
                linking_status = []
                for this_block_file in tasklist:
                    linking_status.append(tasklist[this_block_file])
                if len(set(linking_status)) == 1 and "linked" in set(linking_status):
                    set_hdfs_file(job_id, "worker_" + worker_id + "_status", 'stage2linkingComplete')
                    log.info("Worker [{0}] successfully completed linking tasks for job [{1}]"
                             .format(worker_id, job_id))
                else:
                    set_hdfs_file(job_id, "worker_" + worker_id + "_status", 'stage2linkingFailed')
                log.warning("Worker [{0}] could not complete linking tasks for job [{1}]"
                         .format(worker_id, job_id))
            else:
                log.info("Worker [{0}] found no blockfiles listed for this Datanode in active jobs")
                set_hdfs_file(job_id, "worker_" + worker_id + "_status", 'stage2linkingNotRequired')
            # End non-leader distributed functionality for worker
            #
            # Begin leader workflow for finishing worker tasks
            # if this worker is complete or not required to complete on this job, try to run hdfs delete workflow
            this_job_worker_status = get_hdfs_file(job_id, "worker_" + worker_id + "_status")
            if this_job_worker_status in ['stage2linkingComplete', 'stage2linkingNotRequired']:
                delete_status = worker_leader_delete_hdfs_targets(job_id)
                if delete_status == "success":
                    # Hopefully you have not just become J. Robert Oppenheimer the second.
                    log.info("All worker tasks appear to have completed successfully, shutting down")
                    sys.exit(0)
                elif delete_status == "failed":
                    log.warning("One or more worker tasks have failed, please consult the log for further details")
                    sys.exit(1)
                elif delete_status == "expired":
                    # either the leader job timed out or lost connectivity to the cluster
                    log.info("Worker [{0}] leading job [{1}] leader lease expired without completion, "
                             "please check log and status files to take further administrative action"
                             .format(worker_id, job_id))
                elif delete_status == "notLeader":
                    log.info("Worker [{0}] did not gain leader lease for job [{1}], moving on..."
                             .format(worker_id, job_id))
                else:
                    raise Exception("Worker tasks returned an unexpected status, bailing...")
    else:
        log.info("Worker [{0}] found no jobs in status [stage2copyblocks], shutting down".format(worker_id))
        sys.exit(0)


def shredder_workflow(passed_args):
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


# ###################          End Workflow definitions           ##########################

if __name__ == "__main__":
    # Program setup
    args = init_program(sys.argv[1:])
    # Determine operating mode and execute workflow
    if args.mode is 'client':
        client_workflow(args)
    elif args.mode is 'worker':
        worker_workflow(args)
    elif args.mode is 'shredder':
        shredder_workflow(args)
    else:
        raise "Bad operating mode [{0}] detected. Please consult program help and try again.".format(args.mode)
