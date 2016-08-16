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
from uuid import uuid4, UUID
from socket import gethostname, gethostbyname
from os.path import join as ospathjoin
from os.path import dirname, realpath, ismount, exists, split, isfile
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

# ###################     Global handles     ##########################
zk = None
hdfs = None
worker_id = None

# ###################     Status Flags    ##########################

# Pulling Handle strings up here for easy navigation during code maintenance
status_no_init = "noInit"
status_init = "init"
status_skip = "skip"
status_success = "success"
status_fail = "fail"
status_is_leader = "isLeader"
status_task_timeout = "timeout"

# ###################     Stage Flags    ##########################

stage_1 = "s1"
stage_2 = "s2"
stage_3 = "s3"
stage_4 = "s4"
stage_5 = "s5"
stage_6 = "s6"

# # Stage 2: Worker Initial Leader stage; Takes charge of job and generates blocklists
# stage_2_init = "s2_init"   # Init indicates worker has woken and is attempting leader tasks
# stage_2_not_leader = "s2_nolead"
# stage_2_timeout = "s2_timeout"
# stage_2_leader = "s2_lead"  # active means that this worker is successfully leader and doing tasks
# stage_2_success = "s2_success"
# stage_2_fail = "s2_fail"
# 
# # Stage 3: Worker distributed stage; processes blocklists for jobs and links all shard files on underlying OS
# # Note that the master is not updated for Stage 3, it is a worker only stage
# stage_3_no_init = "s3_noinit" # in blocklist, not processed; in main, worker not started
# stage_3_init = "s3_init" # Worker started, no activity yet
# stage_3_finding_block = "s3_find"  # Attempting to find the block on the OS filesystem
# stage_3_linking_block = "s3_link"  # Attempting to link the found block into the shred control dir
# stage_3_skip = "s3_skip"  # Worker activated to find no blocklist to process
# stage_3_active = "s3_active"  # Worker activated and doing stuff, things and other business
# stage_3_success = "s3_success"  # in blocklist, successfully linked block; in main, worker successfully completed tasks
# stage_3_fail = "s3_fail"  # Something went wrong somewhere in the process, check the log etc

# Stage 4: Worker final leader stage; takes charge of job, validates stage 3 completion, then deletes file from HDFS
stage_4_init = "s4_init"
stage_4_no_init = "s4_noinit"
stage_4_not_leader = "s4_nolead"
stage_4_leader = "s4_lead"
stage_4_active = "s4_active"
stage_4_success = "s4_success"
stage_4_fail = "s4_fail"
stage_4_timeout = "s4_timeout"  # Used when a worker gains leader status but the job takes too long to complete

# Stage 5: Shredder distributed Stage; Validates job status and worker blocklist status, then shreds shards
stage_5_no_init = "s5_noinit"
stage_5_init = "s5_init"
stage_5_active = "s5_active"
stage_5_skip = "s3_skip"  # Worker activated to find no files to shred
stage_5_success = "s5_success"
stage_5_fail = "s5_fail"
stage_5_timeout = "s5_timeout"  # Used when a worker starts shredding but doesn't complete within timeout

# Stage 6: Shredder leader Stage; takes charge of job, checks that all shredders are finished and closes job
stage_6_init = "s6_init"
stage_6_active = "s6_active"
stage_6_success = "s6_success"
stage_6_fail = "s6_fail"
stage_6_timeout = "s6_timeout"  # Used when a worker gains leader status but the job takes too long to complete
stage_6_not_leader = "s6_nolead"
stage_6_no_init = "s6_noinit"

# ###################          Common Functions           ##########################


def ensure_zk():
    """create global connection handle to ZooKeeper"""
    # Construct Host from Config
    host = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
    log.debug("Connecting to Zookeeper using host param [{0}]".format(host))
    global zk
    zk = KazooClient(hosts=host)
    zk.start()
    if zk.state is 'CONNECTED':
        return
    else:
        raise "Could not connect to ZooKeeper with configuration string [{0}], resulting connection state was [{1}]"\
            .format(host, zk.state)


def ensure_hdfs():
    """Uses HDFS client module to connect to HDFS
    returns handle object"""
    if not hdfs:
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


# def set_hdfs_file(job_id, component, content):
#     """Abstracts setting a given status for a given job, job subcomponent, and status message"""
#     # determine file to be written
#     if component == "master":
#         # The master component always updates the state of the job in the master job list
#         # the Master job list is in a separate directory to make it easier to manually deactivate jobs without removing 
#         # their data or history from the store folder
#         file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job_id)
#     elif 'worker' in component or 'data' in component:
#         # Note to maintainers, this is deliberately checking if the strings are in the component name
#         file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, component)
#     else:
#         raise StandardError("Function set_hdfs_file was passed an unrecognised component name")
#     if file_path is not None:
#         log.debug("Setting component file [{0}] at path [{1}] to [{2}]".format(component, file_path, content))
#         hdfs.write(file_path, dumps(content), overwrite=True)
#     else:
#         raise ValueError("File Path not set for function set_hdfs_file with component [{0}] and content [{1}]"
#                          .format(component, content))


# def get_hdfs_file(job_id, component, strict=True):
#     file_content = None
#     if component == "master":
#         # The master component always updates the state of the job in the master job list
#         file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job_id)
#     elif 'worker' in component or 'data' in component:
#         file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, component)
#     else:
#         raise ValueError("Invalid option passed to function get_hdfs_file")
#     try:
#         with hdfs.read(file_path) as reader:
#             # expecting all content by this program to be serialised as json
#             file_content = reader.read()
#     except HdfsError:
#         if strict:
#             raise StandardError("HDFSCli couldn't read a file from path [{0}]"
#                                 .format(file_path))
#         else:
#             pass
#             # if not strict mode then we will return None
#     if file_content:
#         get_result = loads(file_content)
#         log.debug("Retrieved content [{2}] from component file [{0}] at path [{1}]"
#                   .format(component, file_path, get_result))
#     else:
#         get_result = file_content
#     return get_result


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


def parse_user_args(args):
    # TODO: Add option to print out all jobs and states for easy user review
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
    # forcing target to absolute path for safety
    if result.filename:
        temp = realpath(result.filename)
        result.filename = temp
    return result


def get_worker_identity():
    """Determines a unique identity string to use for this worker"""
    # TODO: Implement something more robust than a simple IP lookup!!!
    # worker_id = gethostname()
    worker_id = gethostbyname(gethostname())
    return worker_id


def find_mount_point(path):
    # http://stackoverflow.com/a/4453715
    path = realpath(path)
    while not ismount(path):
        path = dirname(path)
    return path


# ###################          Specific Stage Functions           ##########################


def s1_init_new_job():
    """and generates job management files and dirs"""
    # Generate a guid for a job ID
    ensure_hdfs()
    job_id = str(uuid4())
    log.debug("Generated uuid [{0}] for job identification".format(job_id))
    # Create directory named with a guid, create an initial status file in it for tracking this job
    set_hdfs_file(job_id, 'master', stage_1_init)
    # Create subdir for data
    set_hdfs_file(job_id, 'data_status', stage_1_init)
    # return status and job guid
    return job_id, stage_1_init


def s1_ingest_targets(job_id, source_file):
    """Moves file from initial location to shred worker folder on HDFS"""
    # Update status'
    # TODO: Update sequence to handle list of input files instead of single as a string
    set_hdfs_file(job_id, "master", stage_1_active)
    # we set the state of the data as seperate from the overall job or individual workers
    set_hdfs_file(job_id, "data_status", stage_1_active)
    # Move all files to the data directory
    target_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, 'data')
    # Using the HDFS module's rename function to move the target files
    log.debug("Moving target file [{0}] to shredder holding pen [{1}]".format(source_file, target_path))
    # We need to ensure the directory is created, or the rename command will dump the data into the file
    try:
        hdfs.makedirs(target_path)
        hdfs.rename(source_file, target_path)
        # Sanity checks
        # TODO: Write more sanity checks for ingest process
        source_path, source_filename = split(source_file)
        # set resulting ingested filename for later ease of use
        expected_target_realpath = ospathjoin(target_path, source_filename)
        set_hdfs_file(job_id, "data_filelist", expected_target_realpath)
        # update status
        set_hdfs_file(job_id, "master", stage_1_success)
        set_hdfs_file(job_id, "data_status", stage_1_success)
        return job_id, stage_1_success
    except HdfsError:
        set_hdfs_file(job_id, "master", stage_1_fail)
        set_hdfs_file(job_id, "data_status", stage_1_fail)
        return job_id, stage_1_fail


# def get_target_by_jobid(job_id):
#     """Gets paths of target files ingested into this jobs data store directory
#     returns list of absolute paths to target files on HDFS"""
#     hdfs_file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job_id, "data")
#     # TODO: Consider whether listing the dir is better than the client writing an explicit list and reading it in later
#     log.debug("getting list of files at path [{0}]".format(hdfs_file_path))
#     # hdfs.list returns a list of file names in a directory
#     hdfs_file_list = hdfs.list(hdfs_file_path)
#     out = []
#     for this_file in hdfs_file_list:
#         out.append(ospathjoin(hdfs_file_path, this_file))
#     return out


# def s2_get_fsck_output(target):
#     """Runs HDFS FSCK on the HDFS File to get block location information for Linux shredder"""
#     # fsck_out_iter = run_shell_command(['cat', 'sample-data.txt'])
#     fsck_out_iter = run_shell_command(["hdfs", "fsck", target, "-files", "-blocks", "-locations"])
#     log.debug("Fsck_out type is [{0}]".format(type(fsck_out_iter)))
#     return fsck_out_iter


def parse_fsck_iter(raw_fsck):
    """
    Separate parser for FSCK output to make maintenance easier
    Takes an iterator of the hdfs fsck output
    Returns a dict keyed by IP of each datanode with a list of blk ids
    example: {'172.16.0.80': ['blk_1073839025'], '172.16.0.40': ['blk_1073839025'], '172.16.0.50': ['blk_1073839025']}
    """
    output = {}
    for current_line in raw_fsck:
        if current_line[0].isdigit():
            output_split = current_line.split("[", 1)
            block_id = re.search(':(.+?) ', output_split[0]).group(1).rpartition("_")
            block_by_data_nodes = re.findall("DatanodeInfoWithStorage\[(.*?)\]", output_split[1])
            for block in block_by_data_nodes:
                dn_ip = block.split(":", 1)
                if dn_ip[0] not in output:
                    output[dn_ip[0]] = []
                output[dn_ip[0]].append(block_id[0])
    log.debug("FSCK parser output [{0}]".format(output))
    return output


def get_workers_state_by_job(job_id):
    # check status file of each DN against DNs in blocklist
    worker_list = get_hdfs_file(job_id, "worker_list")
    worker_state_map = {
        stage_3_no_init: [],
        stage_3_init: [],
        stage_3_skip: [],
        stage_3_active: [],
        stage_3_success: [],
        stage_3_fail: [],
        "unexpected": []
    }
    for this_worker in worker_list:
        this_worker_state = get_hdfs_file(job_id, "worker_" + this_worker + "_status")
        if this_worker_state is stage_3_no_init:
            worker_state_map[stage_3_no_init].append(this_worker)
        elif this_worker_state == stage_3_fail:
            worker_state_map[stage_3_fail].append(this_worker)
        elif this_worker_state in [stage_3_success, stage_3_skip, stage_4_leader]:
            worker_state_map[stage_3_success].append(this_worker)
        elif this_worker_state in [stage_3_active, stage_3_init]:
            worker_state_map[stage_3_active].append(this_worker)
        else:
            worker_state_map["unexpected"].append(this_worker)
    log.info("Distributed workers state for job [{0}] is: {1}".format(job_id, dumps(worker_state_map)))
    if len(worker_state_map[stage_3_success]) == len(worker_list):
        result = stage_3_success
    elif len(worker_state_map[stage_3_fail]) > 0 or len(worker_state_map["unexpected"]) > 0:
        result = stage_3_fail
    elif len(worker_state_map[stage_3_no_init]) > 0:
        result = stage_3_no_init
    else:
        result = stage_3_active
    # TODO: Do stuff to validate count and expected names of workers are all correct
    # We want the unique list of validated states, not the detail
    return result


# def get_this_worker_block_dict_by_job(job_id):
#     worker_id = get_worker_identity()
#     ensure_hdfs()
#     # Set status for this worker against this job
#     set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2checkingForBlockList")
#     this_job_worker_block_dict = get_hdfs_file(job_id, ("worker_" + worker_id + "_blockfiledict"))
#     if this_job_worker_block_dict is not None:
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2blocklistFound")
#     else:
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", "stage2noBlocklistFound")
#         # Tasklist should now be either empty, or a dict keyed by block file id
#     return this_job_worker_block_dict


def get_jobs(stage):
    worker_job_list = []
    target_status = []
    if stage == stage_2:
        target_status = [
            stage_1 + "-" + status_success,
            stage_2 + "-" + status_task_timeout
        ]
    elif stage in [stage_3, stage_4]:
        target_status = [
            stage_2 + "-" + status_success,
            stage_4 + "-" + status_task_timeout
        ]
    elif stage in [stage_5, stage_6]:
        target_status = [
            stage_4 + "-" + status_success,
        ]
    # check if dir exists as worker my load before client is ever used
    job_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs")
    job_dir_exists = None
    try:
        # hdfscli strict=False returns None rather than an Error if Dir not found
        job_dir_exists = hdfs.content(job_path, strict=False)
    except AttributeError:
        log.error("HDFS Client not connected")
    if job_dir_exists is not None:
        # if job dir exists, get listing and any files
        dir_listing = hdfs.list(job_path, status=True)
        for item in dir_listing:
            if item[1]['type'] == 'FILE':
                # item[0] is the filename, which for master status' is the job ID as a string
                # we shall be OCD about things and validate it however.
                job_status = retrieve_job_info(item[0], "master")
                if job_status in target_status:
                    try:
                        job_id = UUID(item[0], version=4)
                        worker_job_list.append(str(job_id))
                    except ValueError:
                        pass
    return worker_job_list


def find_shard(shard):
    find_iter = run_shell_command(["find", conf.HDFS_ROOT, "-name", shard])
    found_files = []
    for file in find_iter:
        found_files.append(file.rstrip('\n'))
    # TODO: Handle multiple files found or file missing
    if len(found_files) == 1:
        this_file = found_files[0]
        return this_file
    else:
        raise StandardError("Failed to retrieve path to shard [{0}]".format(shard))


# ###################          Workflow definitions           ##########################

# def s2_main_workflow(job_id):
#     """Attempts to take leadership for job preparation and creates the block-file lists for each datanode worker"""
#     # attempt to kazoo lease new guid node for wait period minutes
#     worker_id = get_worker_identity()
#     ensure_hdfs()
#     set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_2_init)
#     log.debug("Attempting to get leader lease for job [{0}]".format(job_id))
#     ensure_zk()
#     result = None
#     lease_path = conf.ZOOKEEPER['PATH'] + job_id
#     lease = zk.NonBlockingLease(
#         path=lease_path,
#         duration=dttd(minutes=conf.LEADER_WAIT),
#         identifier="Worker [{0}] preparing blocklists".format(worker_id, job_id)
#     )
#     # http://kazoo.readthedocs.io/en/latest/api/recipe/lease.html
#     if not lease:
#         log.debug("Beaten to leasehold by another worker")
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_2_not_leader)
#         result = stage_2_not_leader
#     else:
#         while lease:
#             while result is None:
#                 # Using while result to allow for more detailed failure states in further versions
#                 if zk.state != KazooState.CONNECTED:
#                     log.error("Lost connection to ZooKeeper during leader actions, expiring leader activity")
#                     result = "disconnected"
#                 # Update the state of the distributed workers from their status files on HDFS
#                 log.debug("Worker [{0}] leading job [{1}] preparing blocklists for distributed workers"
#                           .format(worker_id, job_id))
#                 log.debug("Got lease as leader on job, updating job status")
#                 set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_2_leader)
#                 log.debug("Preparing Blocklists for job [{0}]".format(job_id))
#                 # get job target ( returns a list )
#                 targets = get_target_by_jobid(job_id)
#                 log.debug("Got target file(s) [{0}] for job".format(targets))
#                 # get fsck data for targets
#                 blocklists = {}
#                 for target in targets:
#                     fsck_data = s2_get_fsck_output(target)
#                     # parse fsck data for blocklists
#                     blocklists.update(parse_fsck_iter(fsck_data))
#                 log.debug("Parsed FSCK output for target files: [{0}]".format(blocklists))
#                 # match fsck output to worker_ids
#                 # block IDs for workers are currently the IP of the datanode, which matches our worker_id in the utility
#                 # Therefore no current need to do a match between the fsck output and the local worker ID
#                 # However this isn't very robust and should probably be replaced with a better identity matcher
#                 #
#                 # write a per-DN file to hdfs job subdir for other workers to read
#                 target_workers = blocklists.keys()
#                 log.debug("Datanode list for these blockfiles is: [{0}]".format(target_workers))
#                 for this_worker in target_workers:
#                     this_worklist = {}
#                     for blockfile in blocklists[this_worker]:
#                         this_worklist[blockfile] = stage_3_no_init
#                     set_hdfs_file(job_id, "worker_" + this_worker + "_blockfiledict", this_worklist)
#                     # Init the worker status file
#                     set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_3_no_init)
#                 # write list of workers to file for later retrieval
#                 set_hdfs_file(job_id, "worker_list", target_workers)
#                 log.debug("Completed leader tasks for blocklist preparation")
#                 set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_3_no_init)
#                 result = stage_2_success
#             # Broken out of while no result loop, now break lease loop
#             lease = False
#         else:
#             if result is None:
#                 log.warning("Worker timed out preparing blocklist, resetting status for another worker attempt")
#                 set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_2_not_leader)
#                 return stage_2_timeout
#             elif result in [stage_2_success, stage_2_fail]:
#                 # Cleanup lease before returning result
#                 lease = zk.NonBlockingLease(
#                     path=lease_path,
#                     duration=dttd(seconds=1),
#                     identifier="Worker [{0}] preparing blocklists".format(worker_id, job_id)
#                 )
#                 sleep(2)
#                 return result
#             else:
#                 return stage_2_fail


# def s3_main_workflow(job_id, block_list):
#     worker_id = get_worker_identity()
#     # allowing for restart of job where shreddirs was partially completed.
#     potential_shred_files = get_hdfs_file(job_id, "worker_" + worker_id + "_shredfilelist", strict=False)
#     if potential_shred_files is not None:
#         shred_file_list = potential_shred_files
#     else:
#         shred_file_list = {}
#     set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_3_active)
#     for this_block in block_list:
#         if block_list[this_block] in [stage_3_no_init, stage_3_finding_block, stage_3_linking_block]:
#             log.debug("Doing OS Find for blockfile [{0}] on worker [{1}] for job [{2}]"
#                       .format(this_block, worker_id, job_id))
#             # slightly redundant to set init, but if the hdfs root gets more complicated it's good form
#             block_list[this_block] = stage_3_init
#             block_list[this_block] = stage_3_finding_block
#             # TODO: More robust method of getting the HDFS dir root from hdfs-site.xml
#             find_root = conf.HDFS_ROOT
#             find_cmd = ["find", find_root, "-name", this_block]
#             block_find_iter = run_shell_command(find_cmd)
#             # TODO: Handle file not found and other errors
#             found_files = []
#             for file in block_find_iter:
#                 found_files.append(file.rstrip('\n'))
#             # TODO: Handle multiple files found
#             if len(found_files) == 1:
#                 this_file = found_files[0]
#                 log.debug("Found blockfile [{0}] at loc [{1}]".format(this_block, this_file))
#                 block_list[this_block] = stage_3_linking_block
#                 this_file_mount_point = find_mount_point(this_file)
#                 # ensure we have a .shred dir available to link into in this mount point
#                 this_mount_shred_dir = ospathjoin(this_file_mount_point, conf.LINUXFS_SHRED_PATH, job_id)
#                 # Generate expected path + file combo
#                 linked_filepath = ospathjoin(this_mount_shred_dir, this_block)
#                 try:
#                     if not exists(this_mount_shred_dir):
#                         makedirs(this_mount_shred_dir)
#                     # Link the blkfile to it
#                     link(this_file, linked_filepath)
#                     # Record the destination ready for shredding later
#                     shred_file_list[linked_filepath] = stage_5_no_init
#                     log.debug("Linked blockfile [{0}] at loc [{1}] to shred loc at [{2}]"
#                               .format(this_block, this_file, this_mount_shred_dir))
#                     block_list[this_block] = stage_3_success
#                 except:
#                     log.warning("Failed to link blockfile [{0}] at loc [{1}] to shred loc at [{2}]"
#                                 .format(this_block, this_file, this_mount_shred_dir))
#                     block_list[this_block] = stage_3_fail
#             else:
#                 log.error("Found unexpected number of instances of blockfile [{0}] on the local OS filesystem."
#                           .format(this_block))
#                 block_list[this_block] = stage_3_fail
#         elif block_list[this_block] == stage_3_success:
#             log.info("blockfile [{0}] already linked".format(this_block))
#         else:
#             log.warning("blockfile [{0}] in unexpected state [{1}]"
#                         .format(this_block, block_list[this_block]))
#     # update the resulting status of the blockfiles to the job blocklist regardless of outcome
#     set_hdfs_file(job_id, "worker_" + worker_id + "_blockfiledict", block_list)
#     # write out the shred paths for this job for later reference
#     # TODO: Rework to allow for the job to be restarted
#     set_hdfs_file(job_id, "worker_" + worker_id + "_shredfiledict", shred_file_list)
#     # sanity test if linking is completed successfully
#     linking_status = []
#     for this_block in block_list:
#         linking_status.append(block_list[this_block])
#     if len(set(linking_status)) == 1 and stage_3_success in set(linking_status):
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_3_success)
#         return stage_3_success
#     else:
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_3_fail)
#         return stage_3_fail


# def s4_main_workflow(job_id):
#     ensure_zk()
#     ensure_hdfs()
#     worker_id = get_worker_identity()
#     result = None
#     # we use a non-blocking lease for this job, so that another worker can move onto a different job
#     lease_path = conf.ZOOKEEPER['PATH'] + job_id
#     lease = zk.NonBlockingLease(
#         path=lease_path,
#         duration=dttd(minutes=conf.LEADER_WAIT),
#         identifier="Worker [{0}] preparing HDFS delete for job [{1}]".format(worker_id, job_id)
#     )
#     if not lease:
#         log.info("Worker [{0}] did not gain leader lease for job [{1}]".format(worker_id, job_id))
#         result = stage_4_not_leader
#         # reset worker status to init so it can have another go if the current leader drops off without completing
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_4_init)
#     else:
#         log.info("Worker [{0}] gained leader lease for job [{1}]".format(worker_id, job_id))
#         set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_4_leader)
#         # TODO: Refactor with_zk function as wrapper for all leader tasks to reduce duplicated code
#         while lease:
#             while result is None:
#                 if zk.state != KazooState.CONNECTED:
#                     log.error("Lost connection to ZooKeeper during leader actions, expiring leader activity")
#                     result = "disconnected"
#                 # Update the state of the distributed workers from their status files on HDFS
#                 log.debug("Worker [{0}] leading job [{1}] updating state map of distributed workers' linking tasks"
#                           .format(worker_id, job_id))
#                 # GOGO Stage 4
#                 workers_state = get_workers_state_by_job(job_id)
#                 if workers_state == stage_3_success:
#                     # All workers have reported completing stage 3 successfully or are in later stages
#                     log.debug("Worker [{0}] leading job [{1}] all worker linking tasks reporting as completed"
#                               .format(worker_id, job_id))
#                     # TODO: Insert final sanity check before running delete of file from HDFS
#                     set_hdfs_file(job_id, "master", stage_4_init)
#                     set_hdfs_file(job_id, "data_status", stage_4_init)
#                     deletion_target = get_hdfs_file(job_id, "data_filelist")
#                     set_hdfs_file(job_id, "master", stage_4_active)
#                     set_hdfs_file(job_id, "data_status", stage_4_active)
#                     # TODO: Redo this as a list process for multiple file submission
#                     # TODO: Validate state of File and blocks against fresh blocklist in case of changes?
#                     # run hdfs delete files -skiptrash
#                     # Can't use the hdfscli delete command as it doesn't leverage the skiptrash flag
#                     log.debug("Worker [{0}] leading job [{1}] deleting target file [{2}] from hdfs"
#                               .format(worker_id, job_id, deletion_target))
#                     delete_cmd = ['hdfs', 'dfs', '-rm', '-skipTrash', deletion_target]
#                     cmd_output = next(run_shell_command(delete_cmd))
#                     if "Deleted" in cmd_output:
#                         set_hdfs_file(job_id, "data_status", stage_4_success)
#                         set_hdfs_file(job_id, "master", stage_4_success)
#                         result = stage_4_success
#                     else:
#                         log.error("Deletion of file from HDFS returned unexpected result of [{0}], bailing"
#                                   .format(cmd_output))
#                         set_hdfs_file(job_id, "data_status", stage_4_fail)
#                         set_hdfs_file(job_id, "master", stage_4_fail)
#                         result = stage_4_fail
#                 elif workers_state == stage_3_fail:
#                     # raise alerts and instruct log file review
#                     log.warning("Failures reported by workers for job [{0}], bailing...".format(job_id))
#                     set_hdfs_file(job_id, "master", stage_4_fail)
#                     result = stage_4_fail
#                 elif workers_state == stage_3_no_init:
#                     # log that we are still waiting for some workers to start
#                     log.info("Waiting for workers to start for job [{0}], sleeping for [{1}] mins"
#                              .format(job_id, conf.WORKER_WAIT))
#                     sleep(60 * conf.WORKER_WAIT)
#                 elif workers_state == stage_3_active:
#                     # log that workers are in progress, sleep
#                     log.info("Workers for job [{0}] in progress without reported failures, sleeping for [{1}] mins"
#                              .format(job_id, conf.WORKER_WAIT))
#                     sleep(60 * conf.WORKER_WAIT)
#                 else:
#                     # shouldn't be here
#                     log.warning("Unexpected state [{0}] returned from workers for job [{1}], bailing"
#                                 .format(workers_state, job_id))
#                     result = stage_4_fail
#             # We should only get here when result is set to break the outcome loop
#             lease = False
#         else:
#             if result is None:
#                 return stage_4_timeout
#             elif result in [stage_4_success, stage_4_fail]:
#                 # Cleanup lease before returning result
#                 lease = zk.NonBlockingLease(
#                     path=lease_path,
#                     duration=dttd(seconds=1),
#                     identifier="Worker [{0}] preparing blocklists".format(worker_id, job_id)
#                 )
#                 sleep(2)
#                 # Looks like the we completed this task in some manner without faulting, returning the result
#                 return result
#             else:
#                 # A None or unexpected result indicates some kind of weird error and therefore also a failure
#                 return stage_4_fail


def s5_main_workflow(job_id):
    ensure_hdfs()
    worker_id = get_worker_identity()
    result = None
    shred_file_dict = get_hdfs_file(job_id, "worker_" + worker_id + "_shredfiledict")
    if len(shred_file_dict) > 0:
        set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_5_active)
        for target_file in shred_file_dict:
            if shred_file_dict[target_file] in [stage_5_no_init, stage_5_timeout]:
                pass
                # set status to shredding in tasklist
                # run shred
                # set status to shredded in tasklist
            elif shred_file_dict[target_file] == stage_5_success:
                # already done, skip
                pass
            elif shred_file_dict[target_file] == stage_5_active:
                # This shouldn't happen without a weird failure, admin to check
                log.critical("Shredder found file in active shredding state, are you running multiple threads?")
                result = stage_5_fail
            else:
                # Unexpected status, failing
                log.critical("Shredder found a target file [{0}] in an unexpected status of [{1}]"
                             .format(target_file, shred_file_dict[target_file]))
        log.debug("Finished processing shredder tasks for job [{0}]".format(job_id))
        # write back status of shredding regardless of outcome
        set_hdfs_file(job_id, "worker_" + worker_id + "_shredfiledict", shred_file_dict)
        file_statuses = []
        for target_file in shred_file_dict:
            file_statuses.append(shred_file_dict[target_file])
        if len(set(file_statuses)) == 1 and stage_5_success in set(file_statuses):
            set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_5_success)
            result = stage_5_success
        else:
            set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_5_fail)
            result = stage_5_fail
    else:
        set_hdfs_file(job_id, "worker_" + worker_id + "_status", stage_5_skip)
        result = stage_5_skip
    if result is None:
        return stage_5_fail
    else:
        return result


def s6_main_workflow(job_id):
    pass
    # when job complete, set DN status to Stage5complete
    # Move job from incomplete to completed


def persist_job_info(job, component, stage, info):
    if component == "master":
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job)
        content = stage + "-" + info
    elif 'worker' in component or 'data' in component:
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job, component)
        content = dumps(info)
    else:
        raise StandardError("Function persist_job_info was passed an unrecognised component name")
    if file_path is not None:
        try:
            hdfs.write(file_path, content, overwrite=True)
        except HdfsError as e:
            raise e
    else:
        raise ValueError()


def retrieve_job_info(job, component, strict=True):
    file_content = None
    if component == "master":
        # The master component always updates the state of the job in the master job list
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job)
    elif 'worker' in component or 'data' in component:
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job, component)
    else:
        raise ValueError("Invalid option passed to function get_hdfs_file")
    try:
        with hdfs.read(file_path) as reader:
            # expecting all content by this program to be serialised as json
            file_content = reader.read()
    except HdfsError as e:
        if strict:
            raise StandardError("HDFSCli couldn't read a file from path [{0}] with details: {1}"
                                .format(file_path, e))
        else:
            pass
            # if not strict mode then we will return None
    if file_content:
        get_result = loads(file_content)
        log.debug("Retrieved content [{2}] from component file [{0}] at path [{1}]"
                  .format(component, file_path, get_result))
    else:
        get_result = file_content
    return get_result


def run_stage(stage, params=None):
    if stage not in [stage_1, stage_2, stage_3, stage_4, stage_5, stage_6]:
        raise StandardError("Bad stage passed to run_stage")
    ensure_hdfs()
    if stage == stage_1:
        # Stage 1 returns a result and a an ID for the job and has no job list
        target = params
        # TODO: Validate passed file target(s) further, for ex trailing slashes or actually a directory in arg parse
        target_details = hdfs.status(target, strict=False)
        log.debug("HDFS status is: [{0}]".format(target_details))
        if target_details is not None and target_details['type'] == u'FILE':
            job = str(uuid4())
            log.debug("Generated uuid4 [{0}] for job identification".format(job))
            persist_job_info(job, 'master', stage, status_init)
            persist_job_info(job, 'data_status', stage, status_init)
            # TODO: Update to handle list of input files instead of single file as a string
            holding_pen_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job, 'data')
            # Using the HDFS module's rename function to move the target files to test permissions
            log.debug("Moving target file [{0}] to shredder holding pen [{1}]".format(target, holding_pen_path))
            # TODO: Do an are-you-sure, then return no-init if they don't accept
            try:
                # We need to ensure the directory is created, or the rename command will dump the data into the file
                hdfs.makedirs(holding_pen_path)
                hdfs.rename(target, holding_pen_path)
                # TODO: Write more sanity checks for ingest process
                source_path, source_filename = split(target)
                expected_target_real_path = ospathjoin(holding_pen_path, source_filename)
                persist_job_info(job, "data_file_list", stage_1, expected_target_real_path)
                log.debug("Job [{0}] prepared, exiting with success".format(job))
                persist_job_info(job, 'master', stage, status_success)
                persist_job_info(job, 'data_status', stage, status_success)
                return status_success, job
            except HdfsError as e:
                persist_job_info(job, 'master', stage, status_fail)
                persist_job_info(job, 'data_status', stage, status_fail)
                raise StandardError(
                    "Ingestion failed for file [{0}] for job [{1}] with details: {2}"
                    .format(target, job, e)
                )
        else:
            raise StandardError("Submitted File not found on HDFS: [{0}]".format(target))
    else:
        # stages 2 - 6 operate from an active job list predicated by success of the last master stage being successful
        worker = get_worker_identity()
        job_list = get_jobs(stage)
        log.info("Worker [{0}] found [{1}] jobs for stage [{2}]"
                 .format(worker, len(job_list), stage))
        if len(job_list) > 0:
            for job in job_list:
                if stage in [stage_2, stage_4, stage_6]:
                    # Leader Jobs for stages 2, 4, and 6
                    leader_result = None
                    worker_status = (retrieve_job_info(job, "worker_" + worker + "_status")).split("-")[1]
                    if worker_status not in [status_success, status_skip, status_task_timeout]:
                        log.critical(
                            "Worker [{0}] is in status [{1}] for job [{2}], which is not valid to be [{3}] leader."
                            .format(worker, worker_status, job, stage)
                        )
                        leader_result = status_fail
                    persist_job_info(job, 'master', stage, status_init)
                    persist_job_info(job, "worker_" + worker + "_status", stage, status_init)
                    ensure_zk()
                    lease_path = conf.ZOOKEEPER['PATH'] + job
                    lease = zk.NonBlockingLease(
                        path=lease_path,
                        duration=dttd(minutes=conf.LEADER_WAIT),
                        identifier="Worker [{0}] running stage [{1}]".format(worker, stage)
                    )
                    if not lease:
                        leader_result = status_skip
                    else:
                        while lease:
                            while leader_result is None:
                                if zk.state != KazooState.CONNECTED:
                                    log.critical("ZooKeeper disconnected from worker [{0}] during stage [{1}] of job"
                                                 "[{2}], expiring activity"
                                                 .format(worker, stage, job))
                                    leader_result = status_task_timeout
                                persist_job_info(job, "worker_" + worker + "_status", stage, status_is_leader)
                                if stage == stage_2:
                                    target = retrieve_job_info(job, "data_file_list")
                                    master_shard_dict = {}
                                    fsck_iter = run_shell_command(
                                        ["hdfs", "fsck", target, "-files", "-blocks", "-locations"]
                                    )
                                    master_shard_dict.update(parse_fsck_iter(fsck_iter))
                                    target_workers = master_shard_dict.keys()
                                    for this_worker in target_workers:
                                        worker_shard_dict = {}
                                        for shard_file in master_shard_dict[this_worker]:
                                            worker_shard_dict[shard_file] = status_no_init
                                        persist_job_info(
                                            job, "worker_" + worker + "_source_shard_dict", stage, worker_shard_dict
                                        )
                                    persist_job_info(job, "worker_list", stage, target_workers)
                                    leader_result = status_success
                                elif stage in [stage_4, stage_6]:
                                    worker_list = retrieve_job_info(job, "worker_list")
                                    wait = True
                                    while wait is True:
                                        nodes_finished = True
                                        for node in worker_list:
                                            node_stage, node_status = (
                                                retrieve_job_info(job, "worker_" + node + "_status")).split("-")
                                            if (
                                                    node_status == status_fail or  # some node failed something
                                                    stage == stage_4 and node_stage != stage_3 or  # bad stage combo
                                                    stage == stage_6 and node_stage != stage_5  # stage combo breaker!
                                                ):
                                                # This should crash the outer while loop
                                                leader_result = status_fail
                                            elif node_status not in [status_success, status_skip]:
                                                nodes_finished = False
                                        if nodes_finished is True:
                                            wait = False
                                        else:
                                            sleep(60 * conf.WORKER_WAIT)
                                    else:
                                        # We only stop 'wait'ing to start Stage 4/6 if all workers report success
                                        # before the leader lease times out
                                        persist_job_info(job, 'master', stage, status_init)
                                        if stage == stage_4:
                                            persist_job_info(job, 'data_status', stage, status_init)
                                            # TODO: Handle multiple files instead of a single file as string
                                            delete_target = retrieve_job_info(job, "data_file_list")
                                            delete_cmd_result = next(
                                                run_shell_command(['hdfs', 'dfs', '-rm', '-skipTrash', delete_target])
                                            )
                                            if "Deleted" in delete_cmd_result:
                                                persist_job_info(job, 'data_status', stage, status_success)
                                                leader_result = status_success
                                            else:
                                                log.critical(
                                                    "Deletion of file from HDFS returned bad result of [{0}], bailing"
                                                    .format(delete_cmd_result))
                                                persist_job_info(job, 'data_status', stage, status_fail)
                                                leader_result = status_fail
                                        elif stage == stage_6:
                                            pass
                                else:
                                    raise StandardError("Bad stage passed to run_stage")
                            lease = False
                    if leader_result is None or leader_result == status_task_timeout:
                        log.warning(
                            "Worker [{0}] timed out on stage [{1}] leader task, "
                            "resetting status for another worker attempt"
                            .format(worker, stage))
                        persist_job_info(job, "worker_" + worker + "_status", stage, status_task_timeout)
                        persist_job_info(job, 'master', stage, status_task_timeout)
                    elif leader_result in [status_success, status_fail]:
                        # Cleanup lease
                        # TODO: Test if this breaks when the worker test says the worker is in a bad state
                        lease = zk.NonBlockingLease(
                            path=lease_path,
                            duration=dttd(seconds=1),
                            identifier="Worker [{0}] running stage [{1}]".format(worker, stage)
                        )
                        sleep(2)
                        persist_job_info(job, "worker_" + worker + "_status", stage, leader_result)
                        persist_job_info(job, 'master', stage, leader_result)
                    elif leader_result == status_skip:
                        persist_job_info(job, "worker_" + worker + "_status", stage, status_skip)
                    else:
                        raise StandardError("Bad leader_result returned from ZooKeeper wrapper")
                else:
                    # Distributed worker jobs for stage 3 and 5
                    persist_job_info(job, "worker_" + worker + "_status", stage, status_init)
                    if stage == stage_3:
                        source_shard_dict = retrieve_job_info(job, "worker_" + worker + "_source_shard_dict")
                        if source_shard_dict is None:
                            log.debug("Worker [{0}] found no shard list for stage [{1}] in job [{2}]"
                                      .format(worker, stage, job))
                            persist_job_info(job, "worker_" + worker + "_status", stage, status_skip)
                        else:
                            # allowing for restart of job where shard linking was partially completed.
                            linked_shard_dict = retrieve_job_info(
                                job, "worker_" + worker + "_linked_shard_dict", strict=False
                            )
                            if linked_shard_dict is None:
                                linked_shard_dict = {}
                            for shard in source_shard_dict:
                                if source_shard_dict[shard] in [status_no_init, status_init]:
                                    source_shard_dict[shard] = status_init
                                    shard_file_path = find_shard(shard)
                                    shard_file_mount = find_mount_point(shard_file_path)
                                    this_mount_shred_dir = ospathjoin(shard_file_mount, conf.LINUXFS_SHRED_PATH, job)
                                    linked_shard_path = ospathjoin(this_mount_shred_dir, shard)
                                    try:
                                        if not exists(this_mount_shred_dir):
                                            # apparently the exists_ok flag is only in Python2.7+
                                            makedirs(this_mount_shred_dir)
                                        link(shard_file_path, linked_shard_path)
                                        linked_shard_dict[linked_shard_path] = status_no_init
                                        source_shard_dict[shard] = status_success
                                    except OSError as e:
                                        log.critical("Failed to link shard file [{0}] at loc [{1}] to shred loc [{2}]"
                                                     .format(shard, shard_file_path, linked_shard_path))
                                        source_shard_dict[shard] = status_fail
                                elif source_shard_dict[shard] == status_success:
                                    # Already linked, therefore skip
                                    pass
                                else:
                                    raise StandardError(
                                        "Shard control for worker [{0}] on job [{1}] in unexpected state: [{1}]"
                                        .format(worker, job, dumps(source_shard_dict))
                                    )
                            persist_job_info(job, "worker_" + worker + "_source_shard_dict", stage, source_shard_dict)
                            persist_job_info(job, "worker_" + worker + "_linked_shard_dict", stage, linked_shard_dict)
                            # sanity test if linking is completed successfully
                            linking_status = []
                            for shard in source_shard_dict:
                                linking_status.append(source_shard_dict[shard])
                            if len(set(linking_status)) == 1 and status_success in set(linking_status):
                                persist_job_info(job, "worker_" + worker + "_status", stage, status_success)
                            else:
                                persist_job_info(job, "worker_" + worker + "_status", stage, status_fail)
                    else:
                        # must be stage 5
                        pass
            # Now all jobs for stage have run, check all jobs completed successfully before returning
            for job in job_list:
                if stage in [stage_2, stage_4, stage_6]:
                    component = "master"
                else:
                    # must be stage 3 or 5
                    component = "worker_" + worker + "_status"
                job_status = (retrieve_job_info(job, component)).split("-")[1]
                if job_status not in [status_success, status_skip]:
                    log.critical("Worker [{0}] failed or timed out one or more of [{1}] jobs for stage [{2}]"
                                 .format(worker, len(job_list), stage))
                    return status_fail
                log.info("Worker [{0}] found and processed [{1}] jobs for stage [{2}]"
                         .format(worker, len(job_list), stage))
            return status_success
        else:
            return status_skip


# ###################          Begin main definitions           ##########################


def worker_main():
    # worker_id = get_worker_identity()
    # log.info("Worker [{0}] activating".format(worker_id))
    # GOGO Stage 2
    # stage2_jobs = get_jobs_by_status(stage_1_success)
    # if len(stage2_jobs) > 0:
    #     log.info("New jobs found: [{0}]".format(stage2_jobs))
    #     # referring to 's2_job' instead of generic 'job_id' to avoid maintainer confusion
    #     for s2_job in stage2_jobs:
    #         set_hdfs_file(s2_job, "master", stage_2_init)
    #         # Attempt to get lease and run leader active tasks
    #         result = s2_main_workflow(s2_job)
    #         if result == stage_2_success:
    #             log.info("Worker [{0}] successfully completed stage 2 for job [{0}]"
    #                      .format(worker_id, s2_job))
    #             set_hdfs_file(s2_job, "master", stage_2_success)
    #         elif result == stage_2_fail:
    #             set_hdfs_file(s2_job, "master", stage_2_fail)
    #             raise StandardError("Process failed at stage 2 in job [{0}]"
    #                                 "Please refer to log for further details".format(s2_job))
    #         elif result == stage_2_timeout:
    #             # If blocklist job timed out, resetting job to try again with another worker run
    #             log.alert("Worker timed out on stage 2, resetting job to stage_1_success for retry")
    #             set_hdfs_file(s2_job, "master", stage_1_success)
    #         else:
    #             raise StandardError("Unexpected return status from stage 2 task")
    #     log.info("All stage 2 jobs processed, continuing to look for stage 3 jobs...")
    # else:
    #     log.info("No jobs ready for stage 2 found, proceeding to look for stage 3 jobs")
    #     pass
    # # End Stage 2
    #
    # GOGO Stage 3
    # stage3_list = get_jobs_by_status(stage_2_success)
    # if len(stage3_list) > 0:
    #     log.info("Worker [{0}] found active jobs in status [{1}]".format(worker_id, stage_2_success))
    #     for s3_job in stage3_list:
    #         this_worker_block_dict = get_this_worker_block_dict_by_job(s3_job)
    #         if this_worker_block_dict is not None:
    #             set_hdfs_file(s3_job, "worker_" + worker_id + "_status", stage_3_init)
    #             s3_result = s3_main_workflow(s3_job, this_worker_block_dict)
    #         else:
    #             log.info("Worker [{0}] found no blocklist for stage 3 for job [{1}]"
    #                      .format(worker_id, s3_job))
    #             set_hdfs_file(s3_job, "worker_" + worker_id + "_status", stage_3_skip)
    #             s3_result = stage_3_skip
    #         if s3_result in [stage_3_skip, stage_3_success]:
    #             log.info("Worker [{0}] completed stage 3 successfully for job [{1}]"
    #                      .format(worker_id, s3_job))
    #         else:
    #             log.critical("Worker [{0}] failed in one or more stage 3 tasks for job [{1}]."
    #                         "This job will require admin intervention to proceed."
    #                         .format(worker_id, s3_job))
    #     log.info("All stage 3 jobs processed, continuing to look for stage 4 jobs...")
    # else:
    #     log.info("No jobs ready for stage 3 found, proceeding to look for stage 4 jobs")
    #     pass
    # # End Stage 3
    #
    # GOGO Stage 4
    # We are deliberately getting jobs that should be in an Stage 3 state of some sort using stage_2_success
    # # Also running as a separate loop from Stage 3 to avoid cross-worker locking of jobs awaiting progress
    # stage4_list = get_jobs_by_status(stage_2_success)
    # if len(stage4_list) > 0:
    #     for s4_job in stage4_list:
    #         this_worker_status = get_hdfs_file(s4_job, "worker_" + worker_id + "_status")
    #         if this_worker_status not in [stage_3_skip, stage_3_success, stage_4_init, stage_4_timeout]:
    #             log.alert("Worker [{0}] is in status [{2}] for job [{1}], which is not valid to run stage 4 leader."
    #                         "Please check the status of all workers to find and correct any blockage."
    #                         .format(worker_id, s4_job, this_worker_status))
    #             return stage_4_fail
    #         else:
    #             # This worker is in a valid status for s4 leadership
    #             set_hdfs_file(s4_job, "worker_" + worker_id + "_status", stage_4_init)
    #             s4_result = s4_main_workflow(s4_job)
    #             if s4_result == stage_4_success:
    #                 # Hopefully you have not just become J. Robert Oppenheimer the second.
    #                 log.info("All worker tasks completed successfully for job [{0}]".format(s4_job))
    #                 set_hdfs_file(s4_job, "worker_" + worker_id + "_status", stage_4_success)
    #             elif s4_result == stage_4_fail:
    #                 log.warning("One or more worker tasks have failed for job [{0}]"
    #                             "Exiting so the administrator can clean up.".format(s4_job))
    #                 set_hdfs_file(s4_job, "worker_" + worker_id + "_status", stage_4_fail)
    #                 return stage_4_fail
    #             elif s4_result == stage_4_timeout:
    #                 # either the leader job timed out or lost connectivity to the cluster
    #                 log.info("Worker [{0}] lease for leading job [{1}] expired without completion, moving on..."
    #                          .format(worker_id, s4_job))
    #                 set_hdfs_file(s4_job, "worker_" + worker_id + "_status", stage_4_timeout)
    #             elif s4_result == stage_4_not_leader:
    #                 log.info("Worker [{0}] did not gain leader lease for job [{1}], moving on..."
    #                          .format(worker_id, s4_job))
    #                 # worker status stays as stage_4_init
    #             else:
    #                 raise Exception("Worker tasks returned an unexpected status, bailing...")
    #     log.info("All stage 4 jobs processed, exiting...")
    #     return stage_4_success
    # else:
    #     log.info("No jobs ready for stage 4 found, exiting...")
    #     return stage_4_no_init


def shredder_main():
    worker_id = get_worker_identity()
    log.info("Shredder [{0}] activating".format(worker_id))
    ensure_hdfs()
    # GOGO Stage 5
    stage5_jobs = get_jobs_by_status(stage_4_success)
    if len(stage5_jobs) > 0:
        log.info("New jobs found: [{0}]".format(stage5_jobs))
        # referring to 's2_job' instead of generic 'job_id' to avoid maintainer confusion
        for s5_job in stage5_jobs:
            set_hdfs_file(s5_job, "master", stage_5_init)
            result = s5_main_workflow(s5_job)
            if result == stage_5_success:
                log.info("Shredder [{0}] successfully completed stage 5 for job [{0}]"
                         .format(worker_id, s5_job))
            elif result == stage_5_fail:
                raise StandardError("Process failed at stage 2 in job [{0}]"
                                    "Please refer to log for further details".format(s5_job))
            elif result == stage_5_timeout:
                log.alert("Worker timed out on stage 5, job will resume next run")
            else:
                raise StandardError("Unexpected return status from stage 5 task")
        log.info("All stage 5 jobs processed, continuing to look for stage 6 jobs...")
    else:
        log.info("No jobs ready for stage 5 found, proceeding to look for stage 6 jobs")
        pass
    # End Stage 5
    #
    # GOGO Stage 6
    stage6_list = get_jobs_by_status(stage_4_success)
    if len(stage6_list) > 0:
        for s6_job in stage6_list:
            this_worker_status = get_hdfs_file(s6_job, "worker_" + worker_id + "_status")
            if this_worker_status not in [stage_5_skip, stage_5_success, stage_6_init, stage_6_timeout]:
                log.alert("Shredder [{0}] is in status [{2}] for job [{1}], which is not valid to run stage 6 leader."
                          "Please check the status of all shredders to find and correct any blockage."
                          .format(worker_id, s6_job, this_worker_status))
                return stage_6_fail
            else:
                # This worker is in a valid status for s6 leadership
                set_hdfs_file(s6_job, "worker_" + worker_id + "_status", stage_6_init)
                s6_result = s6_main_workflow(s6_job)
                if s6_result == stage_6_success:
                    log.info("All shredder tasks completed successfully for job [{0}]".format(s6_job))
                    set_hdfs_file(s6_job, "worker_" + worker_id + "_status", stage_6_success)
                elif s6_result == stage_6_fail:
                    log.warning("One or more shredder tasks have failed for job [{0}]"
                                "Exiting so the administrator can clean up.".format(s6_job))
                    set_hdfs_file(s6_job, "worker_" + worker_id + "_status", stage_6_fail)
                    return stage_6_fail
                elif s6_result == stage_6_timeout:
                    # either the leader job timed out or lost connectivity to the cluster
                    log.info("Shredder [{0}] lease for leading job [{1}] expired without completion, moving on..."
                             .format(worker_id, s6_job))
                    set_hdfs_file(s6_job, "worker_" + worker_id + "_status", stage_6_timeout)
                elif s6_result == stage_6_not_leader:
                    log.info("Shredder [{0}] did not gain leader lease for job [{1}], moving on..."
                             .format(worker_id, s6_job))
                    # worker status stays as stage_6_init
                else:
                    raise Exception("Shredder tasks returned an unexpected status, bailing...")
        log.info("All stage 6 jobs processed, exiting...")
        return stage_6_success
    else:
        log.info("No jobs ready for stage 6 found, exiting...")
        return stage_6_no_init


def init_program(passed_args):
    log.info("shred.py called with args [{0}]".format(passed_args))
    log.debug("Parsing args using Argparse module.")
    parsed_args = parse_user_args(passed_args)
    # TODO: Move to full configuration file validation function
    log.debug("Checking for config parameters.")
    if not conf.VERSION:
        raise StandardError(
            "Version number in config.py not found, please check configuration file is available and try again."
        )
    # Test necessary connections
    ensure_hdfs()
    # Check directories etc. are setup
    hdfs.makedirs(conf.HDFS_SHRED_PATH)
    # TODO: Further Application setup tests
    return parsed_args


# ###################          main program           ##########################

if __name__ == "__main__":
    args = init_program(sys.argv[1:])
    result = None
    if args.mode is 'client':
        result, new_job_id = run_stage(stage=stage_1, params=args.filename)
    elif args.mode is 'worker':
        stage_result = status_skip
        while stage_result in [status_skip, status_success]:
            for stage in [stage_2, stage_3, stage_4]:
                stage_result = run_stage(stage)
            sys.exit(0)
        else:
            sys.exit(1)
    elif args.mode is 'shredder':
        result = shredder_main()
    else:
        StandardError("Bad operating mode [{0}] detected. Please consult program help and try again.".format(args.mode))
    if result is not None and result in [status_success, status_no_init]:
        sys.exit(0)
    else:
        sys.exit(1)