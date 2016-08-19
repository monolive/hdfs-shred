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
from os.path import split as ospathsplit
from os.path import dirname, realpath, ismount, exists
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

# ###################     Status and stage Flags    ##########################

# Pulling Handle strings up here for easy navigation during code maintenance
# These strings cannot use the character '-' as it is used to concatenate status strings

status_no_init = "noInit"
status_init = "init"
status_skip = "skip"
status_success = "success"
status_fail = "fail"
status_is_leader = "isLeader"
status_task_timeout = "timeout"

stage_1 = "s1"  # Init job and quarantine target HDFS files for later shredding. Tests user permissions also
stage_2 = "s2"  # A single worker prepares central lists of shard files on nodes for later distributed processing
stage_3 = "s3"  # All workers on each node containing shard files link them in local FS for later shredding
stage_4 = "s4"  # A single worker monitors for all worker s3 success, then deletes the target from HDFS
stage_5 = "s5"  # All workers on each node containing shard files now shred the files
stage_6 = "s6"  # A single worker monitors for all worker s5 success, then closes and archives the job

# ###################          Functions           ##########################


def parse_user_args(user_args):
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
    log.debug("Parsing commandline args [{0}]".format(user_args))
    result = parser.parse_args(user_args)
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


def ensure_zk():
    """create global connection handle to ZooKeeper"""
    global zk
    zk_host = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
    if not zk or zk.state != 'CONNECTED':
        log.debug("Connecting to Zookeeper using host param [{0}]".format(zk_host))
        zk = KazooClient(hosts=zk_host)
        zk.start()
    if zk.state is 'CONNECTED':
        return
    else:
        raise EnvironmentError("Could not connect to ZooKeeper with configuration string [{0}],"
                               " resulting connection state was [{1}]".format(zk_host, zk.state))


def ensure_hdfs():
    """Uses HDFScli to connect to HDFS returns handle object"""
    global hdfs
    if not hdfs:
        log.debug("Attempting to instantiate HDFS client")
        # TODO: Write try/catch for connection errors and states
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


def run_shell_command(command, return_iter=True):
    """Read output of shell command
    returns an iterator or manages single line/null response"""
    log.debug("Running Shell command [{0}]".format(command))
    # http://stackoverflow.com/a/13135985
    p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
    if return_iter:
        return iter(p.stdout.readline, b'')
    else:
        line = p.stdout.readline()
        if line != '':
            return line.rstrip()
        else:
            return None


def get_worker_identity():
    """Determines a unique identity string to use for this worker"""
    # TODO: Implement something more robust than a simple IP lookup!!!
    worker_id = gethostbyname(gethostname())
    return worker_id


def find_mount_point(file_path):
    # http://stackoverflow.com/a/4453715
    file_path = realpath(file_path)
    while not ismount(file_path):
        file_path = dirname(file_path)
    return file_path


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


def get_jobs(stage):
    """Prepares a cleaned job list suitable for the stage requested from all active jobs
    returns list of job UUID4 strings"""
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
            stage_6 + "-" + status_task_timeout
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
    """Finds a file in the local node filesystem, used to find shards in the local HDFS directory"""
    find_iter = run_shell_command(["find", conf.HDFS_ROOT, "-name", shard])
    found_files = []
    for line in find_iter:
        found_files.append(line.rstrip('\n'))
    # TODO: Handle multiple files found or file missing
    if len(found_files) == 1:
        this_file = found_files[0]
        return this_file
    else:
        raise StandardError("Failed to retrieve path to shard [{0}]".format(shard))


def persist_job_info(job, component, stage, info):
    """Writes data to our directory structure in the HDFS shred directory"""
    if component == "master":
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "jobs", job)
        content = dumps(stage + "-" + info)
    elif 'worker' in component or 'data' in component:
        file_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job, component)
        if 'status' in component:
            content = dumps(stage + "-" + info)
        else:
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
    """Retrieves data stored in our HDFS Shred directory"""
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


# ###################          Main Workflows           ##########################


def init_program(passed_args):
    log.info("shred.py called with args [{0}]".format(passed_args))
    parsed_args = parse_user_args(passed_args)
    # TODO: Further configuration file validation tests
    log.debug("Checking for config parameters.")
    if not conf.VERSION:
        raise StandardError(
            "Version number in config.py not found, please check configuration file is available and try again."
        )
    # Test necessary connections
    ensure_hdfs()
    # Check directories etc. are setup
    hdfs.makedirs(ospathjoin(conf.HDFS_SHRED_PATH, "jobs"))
    hdfs.makedirs(ospathjoin(conf.HDFS_SHRED_PATH, "store"))
    # TODO: Further Application setup tests
    return parsed_args


def run_stage(stage, params=None):
    """
    Main program logic
    As many stages share a lot of similar functionality, they are interleved using the 'stage' parameter as a selector
    Stages should be able to run independently for testing or admin convenience
    """
    ensure_hdfs()
    if stage == stage_1:
        # Stage 1 returns a result and a an ID for the job and has no job list
        target = params
        # TODO: Validate passed file target(s) further, for ex trailing slashes or actually a directory in arg parse
        job = str(uuid4())
        log.debug("Generated uuid4 [{0}] for job identification".format(job))
        persist_job_info(job, 'master', stage, status_init)
        persist_job_info(job, 'data_status', stage, status_init)
        holding_pen_path = ospathjoin(conf.HDFS_SHRED_PATH, "store", job, 'data')
        source_path, source_filename = ospathsplit(target)
        expected_target_real_path = ospathjoin(holding_pen_path, source_filename)
        try:
            # TODO: Update to handle list of input files instead of single file as a string
            target_details = hdfs.status(target)
            if target_details['type'] == u'FILE':
                # We need to ensure the directory is created, or the rename command will dump the data into the file
                hdfs.makedirs(holding_pen_path)
                # Using the HDFS module's rename function to move the target files to test permissions
                log.debug("Moving target file [{0}] to shredder holding pen [{1}]".format(target, holding_pen_path))
                # TODO: Do an are-you-sure, then return status_skip if they don't accept
                hdfs.rename(target, holding_pen_path)
                # TODO: Write more sanity checks for ingest process
                persist_job_info(job, "data_file_list", stage_1, expected_target_real_path)
                log.debug("Job [{0}] prepared, exiting with success".format(job))
                persist_job_info(job, 'master', stage, status_success)
                persist_job_info(job, 'data_status', stage, status_success)
                return status_success, job
            else:
                log.critical("Target is not valid, type returned was [{0}]".format(target_details['type']))
                persist_job_info(job, 'master', stage, status_fail)
                persist_job_info(job, 'data_status', stage, status_fail)
                return status_fail, job
        except HdfsError as e:
            persist_job_info(job, 'master', stage, status_fail)
            persist_job_info(job, 'data_status', stage, status_fail)
            log.critical("Ingestion failed for file [{0}] for job [{1}] with details: {2}"
                         .format(target, job, e))
            return status_fail, job
    elif stage in [stage_2, stage_3, stage_4, stage_5, stage_6]:
        # stages 2 - 6 operate from an active job list predicated by success of the last master stage
        worker = get_worker_identity()
        job_list = get_jobs(stage)
        log.info("Worker [{0}] found [{1}] jobs for stage [{2}]".format(worker, len(job_list), stage))
        if len(job_list) > 0:
            for job in job_list:
                if stage in [stage_2, stage_4, stage_6]:
                    # Leader Jobs for stages 2, 4, and 6
                    # We use the absence of a leader_result to control activity within leader tasks
                    leader_result = None
                    # Worker may not yet have status file initialised for s2 of job
                    worker_status = (retrieve_job_info(job, "worker_" + worker + "_status", strict=False))
                    # TODO: Move worker state validation to a seperate function returning a t/f against worker/stage
                    if (
                        (worker_status is None and stage != stage_2) or
                        (worker_status is not None and worker_status not in [
                            stage_3 + "-" + status_success, stage_3 + "-" + status_skip, stage_4 + "-" + status_task_timeout,
                            stage_5 + "-" + status_success, stage_5 + "-" + status_skip, stage_6 + "-" + status_task_timeout,
                    ])):
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
                                        # TODO: Do stuff to validate count and expected names of workers are all correct
                                        nodes_finished = True
                                        for node in worker_list:
                                            node_stage, node_status = (
                                                retrieve_job_info(job, "worker_" + node + "_status")).split("-")
                                            if (
                                                node_status == status_fail or  # some node failed something
                                                stage == stage_4 and node_stage != stage_3 or  # bad stage combo
                                                stage == stage_6 and node_stage != stage_5  # stage combo breaker!
                                            ):
                                                # This should crash the outer while loop to fail this process
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
                                            # TODO: Validate against fresh blocklist in case of changes?
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
                                            # All workers have completed shredding, shut down job and clean up
                                            # TODO: Test that job completed as expected
                                            leader_result = status_success
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
                        _ = zk.NonBlockingLease(
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
                elif stage in [stage_3, stage_5]:
                    # Distributed worker jobs for stage 3 and 5
                    persist_job_info(job, "worker_" + worker + "_status", stage, status_init)
                    if stage == stage_3:
                        targets_dict = retrieve_job_info(job, "worker_" + worker + "_source_shard_dict", strict=False)
                        # allowing for restart of job where shard linking was partially completed.
                        linked_shard_dict = retrieve_job_info(
                            job, "worker_" + worker + "_linked_shard_dict", strict=False
                        )
                        if linked_shard_dict is None:
                            linked_shard_dict = {}
                    elif stage == stage_5:
                        targets_dict = retrieve_job_info(job, "worker_" + worker + "_linked_shard_dict", strict=False)
                    else:
                        raise StandardError("Bad code pathway")
                    if targets_dict is None:
                        log.debug("Worker [{0}] found no shard list for stage [{1}] in job [{2}]"
                                  .format(worker, stage, job))
                        persist_job_info(job, "worker_" + worker + "_status", stage, status_skip)
                    else:
                        for shard in targets_dict:
                            if targets_dict[shard] in [status_no_init, status_init]:
                                targets_dict[shard] = status_init
                                if stage == stage_3:
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
                                        targets_dict[shard] = status_success
                                    except OSError as e:
                                        log.critical("Failed to link shard file [{0}] at loc [{1}] to shred loc [{2}]"
                                                     .format(shard, shard_file_path, linked_shard_path))
                                        targets_dict[shard] = status_fail
                                elif stage == stage_5:
                                    # Shred returns 0 on success and a 'failed' message on error
                                    # run_shell_command handles this behavior for us
                                    # TODO: Insert final sanity check before shredding files
                                    shred_result = run_shell_command(
                                        ['shred', '-n', str(conf.SHRED_COUNT), '-z', '-u', shard],
                                        return_iter=False
                                    )
                                    if shred_result is not None:
                                        log.critical("Worker [{0}] failed to shred shard [{1}] with error: {2}"
                                                     .format(worker, shard, shred_result))
                                        targets_dict[shard] = status_fail
                                    else:
                                        targets_dict[shard] = status_success
                            elif targets_dict[shard] == status_success:
                                # Already done, therefore skip
                                pass
                            else:
                                raise StandardError(
                                    "Shard control for worker [{0}] on job [{1}] in unexpected state: [{1}]"
                                    .format(worker, job, dumps(targets_dict))
                                )
                        if stage == stage_3:
                            persist_job_info(job, "worker_" + worker + "_source_shard_dict", stage, targets_dict)
                            persist_job_info(job, "worker_" + worker + "_linked_shard_dict", stage, linked_shard_dict)
                        if stage == stage_5:
                            persist_job_info(job, "worker_" + worker + "_linked_shard_dict", stage, targets_dict)
                        # sanity test if task is completed successfully
                        target_status = []
                        for shard in targets_dict:
                            target_status.append(targets_dict[shard])
                        if len(set(target_status)) == 1 and status_success in set(target_status):
                            persist_job_info(job, "worker_" + worker + "_status", stage, status_success)
                        else:
                            persist_job_info(job, "worker_" + worker + "_status", stage, status_fail)
                else:
                    # Shouldn't be able to get here
                    raise StandardError("Bad stage definition passed to run_stage: {0}".format(stage))
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
            # No jobs found for this stage/worker
            return status_skip
    else:
        raise StandardError("Bad stage definition passed to run_stage: {0}".format(stage))


# ###################          main program           ##########################

if __name__ == "__main__":
    args = init_program(sys.argv[1:])
    stage_list = []
    if args.mode is 'client':
        stage_list = [stage_1, ]
    elif args.mode is 'worker':
        stage_list = [stage_2, stage_3, stage_4]
    elif args.mode is 'shredder':
        stage_list = [stage_5, stage_6]
    else:
        StandardError("Bad operating mode [{0}] detected. Please consult program help and try again.".format(args.mode))
    stage_result = status_skip
    while stage_result in [status_skip, status_success]:
        for this_stage in stage_list:
            if this_stage == stage_1:
                stage_result, new_job_id = run_stage(stage=stage_1, params=args.filename)
            else:
                stage_result = run_stage(this_stage)
        sys.exit(0)
    else:
        sys.exit(1)
