#!/usr/bin/python
# -*- coding: utf-8 -*-

from os.path import join as ospathjoin
from glob import glob
from shlex import split as ssplit
import argparse
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
import pytest
import shred
import logging
import collections
import socket


test_file_size = "10"
test_file_path = "/tmp/"
test_file_dir = "shred_test_file"
test_files = []
# Each test should probably have independent job IDs and data for safety
# test_job_id = None
# test_job_status = None

# Test control Params
remove_test_files = False
remove_test_zkdata = True
remove_test_jobs = True

# Config overrides
shred.log.setLevel(shred.logging.DEBUG)
shred.conf.ZOOKEEPER['PATH'] = '/testshred/'
shred.conf.LINUXFS_SHRED_PATH = ".testshred"
shred.conf.HDFS_SHRED_PATH = "/tmp/testshred"

# ###################          Test Environment setup   ##########################


def setup_module():
    # Moved data setup and cleanup to individual tests
    # It's slower overall, but cleaner if tests are to be run individually
    pass


def teardown_module():
    shred.log.info("Begin Teardown...")
    # Remove test data
    if remove_test_files:
        shred.log.info("Removing Test Data")
        rmdir_cmd = ["hdfs", "dfs", "-rm", "-f", "-r", "-skipTrash", ospathjoin(test_file_path, test_file_dir)]
        rmdir_iter = shred.run_shell_command(rmdir_cmd)
        for line in rmdir_iter:
            shred.log.info(line)
    else:
        shred.log.info("Skipping removal of test data")
    if remove_test_zkdata:
        shred.log.info("Removing test ZK Data")
        shred.ensure_zk()
        shred.zk.delete(path=shred.conf.ZOOKEEPER['PATH'], recursive=True)
    else:
        shred.log.info("Skipping removal of test ZK Data")
    if remove_test_jobs:
        clear_test_jobs()
    else:
        shred.log.info("Skipping removal of test jobs")


# ###################          Test Data Controls   ##########################


def clear_test_jobs():
    shred.log.info("Removing test jobs")
    rmdir_cmd = ["hdfs", "dfs", "-rm", "-f", "-r", "-skipTrash", ospathjoin(shred.conf.HDFS_SHRED_PATH)]
    rmdir_iter = shred.run_shell_command(rmdir_cmd)
    for line in rmdir_iter:
        shred.log.info(line)
    # TODO: This should also check and remove blockfiles moved to the .shredtest dir on each mount


def generate_test_data():
    # Generate test data
    shred.log.info("Out of test data, generating new Test Data files...")
    clean_dir_cmd = ["hdfs", "dfs", "-rmdir", ospathjoin(test_file_path, test_file_dir)]
    shred.run_shell_command(clean_dir_cmd)
    gen_test_data_cmd = ["/usr/hdp/current/hadoop-client/bin/hadoop", "jar",
                         glob("/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples-*.jar")[0],
                         "teragen", test_file_size, ospathjoin(test_file_path, test_file_dir)]
    gen_test_data_iter = shred.run_shell_command(gen_test_data_cmd)
    for line in gen_test_data_iter:
        if "Bytes Written" in line:
            shred.log.info(line)
    # remove 0 size '_SUCCESS' file
    del_file_cmd = ["hdfs", "dfs", "-rm", ospathjoin(test_file_path, test_file_dir, "_SUCCESS")]
    do_nothing = shred.run_shell_command(del_file_cmd)


def update_test_files():
    shred.log.info("Updating list of test data files")
    global test_files
    filelist_cmd = ["hdfs", "dfs", "-ls", ospathjoin(test_file_path, test_file_dir)]
    filelist_iter = shred.run_shell_command(filelist_cmd)
    for line in filelist_iter:
        splits = ssplit(line)
        if "{0}".format(ospathjoin(test_file_path, test_file_dir)) in splits[-1]:
            test_files.append(splits[-1])
    shred.log.info("Test files are now [{0}]".format(test_files))


def get_test_file():
    try:
        test_file = test_files.pop()
    except IndexError:
        generate_test_data()
        update_test_files()
        test_file = test_files.pop()
    return test_file


# ###################          Main workflow tests            ##########################


@pytest.mark.slow
def test_init_program():
    # This simply tests the code pathway in init_program; parse args is tested independently
    test_file = get_test_file()
    test_args = ["-m", "client", "-f", test_file]
    args = shred.init_program(test_args)
    assert args.mode == "client"
    assert args.filename == test_file
    path_test = shred.hdfs.status(shred.conf.HDFS_SHRED_PATH)
    assert path_test['type'] == "DIRECTORY"


def test_run_stage_1():
    # test a bad file
    result, test_job_id = shred.run_stage(shred.stage_1, 'not_a_file')
    assert result == shred.status_fail
    job_status = shred.retrieve_job_info(test_job_id, "master")
    assert shred.status_fail in job_status
    data_status = shred.retrieve_job_info(test_job_id, "data_status")
    assert shred.status_fail in data_status
    # test a good file
    test_file = get_test_file()
    result, test_job_id = shred.run_stage(shred.stage_1, test_file)
    assert result == shred.status_success
    job_status = shred.retrieve_job_info(test_job_id, "master")
    assert shred.status_success in job_status
    data_status = shred.retrieve_job_info(test_job_id, "data_status")
    assert shred.status_success in data_status
    # Only tests a single file
    target = shred.retrieve_job_info(test_job_id, "data_file_list")
    target_exists = shred.hdfs.status(target, strict=False)
    assert target_exists is not None


@pytest.mark.slow
def test_worker_main():
    shred.log.info("Clearing all test jobs to prepare an uncluttered test run for worker_main()")
    clear_test_jobs()
    result = shred.worker_main()
    assert result == shred.stage_4_no_init
    shred.log.info("Generating a new job to test worker_main()")
    test_file = get_test_file()
    result, test_job_id = shred.client_main(test_file)
    assert result == shred.stage_1_success
    shred.log.info("Running test of worker main with single active job in s1_success state")
    result = shred.worker_main()
    assert result == shred.stage_4_success


# ###################          Individual Function tests            ##########################


@pytest.mark.slow
def test_parse_user_args():
    shred.log.info("Testing argparse")
    out = shred.parse_user_args(["-m", "client", "-f", "somefile"])
    assert out.filename == "somefile"
    assert out.mode == "client"
    out = shred.parse_user_args(["-m", "worker"])
    assert out.mode == "worker"
    assert out.filename is None
    out = shred.parse_user_args(["-m", "shredder"])
    assert out.mode == "shredder"
    assert out.filename is None
    with pytest.raises(SystemExit):
        shred.parse_user_args(["-m", "file"])
    with pytest.raises(SystemExit):
        shred.parse_user_args(["-m", "worker", "-f", "somefile"])
    with pytest.raises(SystemExit):
        shred.parse_user_args(["-v"])
    with pytest.raises(SystemExit):
        shred.parse_user_args(["-h"])


@pytest.mark.slow
def test_ensure_zk():
    shred.log.info("Testing ZooKeeper connector")
    # Good host
    shred.ensure_zk()
    assert shred.zk.state == 'CONNECTED'
    shred.zk.stop()
    assert shred.zk.state == 'LOST'


@pytest.mark.slow
def test_ensure_hdfs():
    shred.log.info("Testing Connection to HDFS")
    shred.ensure_hdfs()
    # TODO: Test HDFS connectivity stability
    # TODO: This really needs connection tracking


@pytest.mark.slow
def test_run_shell_command():
    pass


@pytest.mark.slow
def test_get_worker_identity():
    worker_id = shred.get_worker_identity()
    # testing is a valid IP returned
    socket.inet_aton(worker_id)


@pytest.mark.slow
def test_find_mount_point():
    pass


@pytest.mark.slow
def test_parse_fsck_iter():
    shred.log.info("Testing FSCK parser")
    test_file = get_test_file()
    fsck_content = shred.s2_get_fsck_output(test_file)
    out = shred.parse_fsck_iter(fsck_content)
    assert isinstance(out, dict)


@pytest.mark.slow
def test_get_jobs():
    pass


@pytest.mark.slow
def test_find_shard():
    pass


@pytest.mark.slow
def test_persist_job_info():
    pass


@pytest.mark.slow
def test_retrieve_job_info():
    pass


# ###################          Data fuzzing tests            ##########################

# TODO: Test for 0 size files that make fsck behave differently
