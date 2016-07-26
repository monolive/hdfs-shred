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
test_job_id = None
test_job_status = None

# Test control Params
remove_test_files = False
remove_test_zkdata = True

# Config overrides
shred.log.setLevel(shred.logging.DEBUG)
shred.conf.ZOOKEEPER['PATH'] = '/shredtest/'


# ###################          Test environment setup functions            ##########################


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


def setup_module():
    shred.log.info("Begin Setup")
    shred.log.info("Checking for existing test data")
    # Check if test data already exists
    test_data_exists_cmd = ["hdfs", "dfs", "-ls", ospathjoin(test_file_path, test_file_dir)]
    test_data_exists_iter = shred.run_shell_command(test_data_exists_cmd)
    try:
        out = next(test_data_exists_iter)
        if "No such file or directory" in out:
            test_data_exists_state = False
        else:
            test_data_exists_state = True
    except StopIteration:
        test_data_exists_state = False
    if test_data_exists_state is False:
        generate_test_data()
    update_test_files()


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
        zk_host = shred.conf.ZOOKEEPER['HOST'] + ':' + str(shred.conf.ZOOKEEPER['PORT'])
        zk = shred.connect_zk()
        zk.delete(path=shred.conf.ZOOKEEPER['PATH'], recursive=True)
    else:
        shred.log.info("Skipping removal of test ZK Data")


# ###################          Individual Function tests            ##########################


def test_parse_args():
    shred.log.info("Testing argparse")
    out = shred.parse_args(["-m", "client", "-f", "somefile"])
    assert out.filename == "somefile"
    assert out.mode == "client"
    out = shred.parse_args(["-m", "worker"])
    assert out.mode == "worker"
    assert out.filename is None
    out = shred.parse_args(["-m", "shredder"])
    assert out.mode == "shredder"
    assert out.filename is None
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-m", "file"])
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-m", "worker", "-f", "somefile"])
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-v"])
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-h"])


def test_connect_zk():
    shred.log.info("Testing ZooKeeper connector")
    # Good host
    zk = shred.connect_zk()
    assert zk.state == 'CONNECTED'


def test_get_fsck_output():
    shred.log.info("Testing FSCK information fetcher")
    out = shred.get_fsck_output(test_files[-1])
    assert isinstance(out, collections.Iterator)


def test_parse_blocks_from_fsck():
    shred.log.info("Testing FSCK parser")
    fsck_content = shred.get_fsck_output(test_files[-1])
    out = shred.parse_blocks_from_fsck(fsck_content)
    assert isinstance(out, dict)
    # TODO: Test dictionary entries for IP and list of blk_<num> entries


def test_connect_hdfs():
    shred.log.info("Testing Connection to HDFS")
    hdfs = shred.connect_hdfs()
    # TODO: Write tests here


def test_check_hdfs_for_target():
    shred.log.info("Testing HDFS File checker")
    hdfs = shred.connect_hdfs()
    # Test good file
    out = shred.check_hdfs_for_target(test_files[-1])
    assert out is True
    # Test bad file
    out = shred.check_hdfs_for_target('/tmp/notafile')
    assert out is False
    # test passing a dir
    out = shred.check_hdfs_for_target('/tmp')
    assert out is False


def test_init_new_job():
    shred.log.info("Testing Job Preparation Logic")
    hdfs = shred.connect_hdfs()
    job_id, job_status = shred.init_new_job()
    assert job_id is not None
    assert job_status == "stage1init"

    shred.log.info("Testing Target File Ingest")
    my_test_file = test_files.pop()
    job_id, job_status = shred.ingest_targets(job_id, my_test_file)
    # this removes the test file from availability, so we have to pop it off the queue
    assert job_id is not None
    assert job_status == "stage1ingestComplete"

    shred.finalise_client(job_id, my_test_file)


def test_get_worker_identity():
    worker_id = shred.get_worker_identity()
    # testing is a valid IP returned
    socket.inet_aton(worker_id)


def test_prepare_blocklists():
    # TODO: Rewrite Main workflow in shred.py into a wrapper function for each core component for easier testing
    try:
        my_test_file = test_files.pop()
    except IndexError:
        generate_test_data()
        update_test_files()
        my_test_file = test_files.pop()
    if not test_job_id:
        global test_job_id
        hdfs = shred.connect_hdfs()
        test_job_id, test_job_status = shred.init_new_job()
        test_job_id, test_job_status = shred.ingest_targets(test_job_id, my_test_file)
        shred.finalise_client(test_job_id, my_test_file)
    test_worklist = shred.get_jobs_by_status('stage1complete')
    assert test_worklist > 0
    zk = shred.connect_zk()
    test_result = shred.prepare_blocklists(test_job_id)
    assert test_result == "success"


# ###################          Data fuzzing tests            ##########################

# TODO: Test for 0 size files that make fsck behave differently


# ###################          Workflow Tests            ##########################