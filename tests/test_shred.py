#!/usr/bin/python
# -*- coding: utf-8 -*-

from os.path import join
from glob import glob
from shlex import split as ssplit
import argparse
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
import pytest
import shred
import logging
import collections


test_file_size = "10"
test_file_path = "/tmp/"
test_file_name = "shred_test_file"
test_files = []

# Test control Params
remove_test_files = False
remove_test_zkdata = True

# Config overrides
shred.log.setLevel(shred.logging.DEBUG)
shred.conf.ZOOKEEPER['PATH'] = '/shredtest/'


def setup_module():
    shred.log.info("Begin Setup")
    # Check if test data already exists
    test_data_exists_cmd = ["hdfs", "dfs", "-stat", join(test_file_path, test_file_name)]
    test_data_exists_iter = shred.run_shell_command(test_data_exists_cmd)
    test_data_exists_resp = next(test_data_exists_iter)
    if "No such file or directory" in test_data_exists_resp:
        test_data_exists_state = False
        shred.log.info("Test Data not Found")
    else:
        test_data_exists_state = True
        shred.log.info("Test Data already exists")
    if test_data_exists_state is False:
        # Generate test data
        shred.log.info("Generating Test Data...")
        gen_test_data_cmd = ["/usr/hdp/current/hadoop-client/bin/hadoop", "jar",
                   glob("/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples-*.jar")[0],
                   "teragen", test_file_size, join(test_file_path, test_file_name)]
        gen_test_data_iter = shred.run_shell_command(gen_test_data_cmd)
        for line in gen_test_data_iter:
            if "Bytes Written" in line:
                shred.log.info(line)
    # get test files
    shred.log.info("Getting list of Test Files")
    filelist_cmd = ["hdfs", "dfs", "-ls", join(test_file_path, test_file_name)]
    filelist_iter = shred.run_shell_command(filelist_cmd)
    for line in filelist_iter:
        splits = ssplit(line)
        if "{0}".format(join(test_file_path, test_file_name)) in splits[-1]:
            test_files.append(splits[-1])
    print test_files


def teardown_module():
    shred.log.info("Begin Teardown...")
    # Remove test data
    if remove_test_files:
        shred.log.info("Removing Test Data")
        rmdir_cmd = ["hdfs", "dfs", "-rm", "-f", "-r", "-skipTrash", join(test_file_path, test_file_name)]
        rmdir_iter = shred.run_shell_command(rmdir_cmd)
        for line in rmdir_iter:
            shred.log.info(line)
    else:
        shred.log.info("Skipping removal of test data")
    if remove_test_zkdata:
        shred.log.info("Removing test ZK Data")
        zk_host = shred.conf.ZOOKEEPER['HOST'] + ':' + str(shred.conf.ZOOKEEPER['PORT'])
        zk_conn = shred.connect_zk(zk_host)
        zk_conn.delete(path=shred.conf.ZOOKEEPER['PATH'], recursive=True)
    else:
        shred.log.info("Skipping removal of test ZK Data")


def test_parse_args():
    shred.log.info("Testing argparse")
    out = shred.parse_args(["-m", "file", "-f", "somefile"])
    assert out.filename == "somefile"
    assert out.mode == "file"
    out = shred.parse_args(["-m", "blocks"])
    assert out.mode == "blocks"
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-m", "file"])
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-v"])
    with pytest.raises(SystemExit):
        out = shred.parse_args(["-h"])


def test_check_hdfs_compat():
    shred.log.info("Testing HDFS Compatibility - Placeholder")
    # TODO: Write more meaningful test for this function
    out = shred.check_hdfs_compat()
    assert out == True


def test_connect_zk():
    shred.log.info("Testing ZooKeeper connector")
    # Bad Host
    with pytest.raises(KazooTimeoutError):
        out = shred.connect_zk('badhost:25531')
    # Good host
    zk_host = shred.conf.ZOOKEEPER['HOST'] + ':' + str(shred.conf.ZOOKEEPER['PORT'])
    zk = shred.connect_zk(zk_host)
    assert zk.state == 'CONNECTED'


def test_check_hdfs_for_file():
    shred.log.info("Testing HDFS File checker")
    # Test good file
    out = shred.check_hdfs_for_file(test_files[-1])
    assert out is True
    # Test bad file
    with pytest.raises(ValueError):
        shred.check_hdfs_for_file('/tmp/notafile')
    # test passing a dir
    with pytest.raises(ValueError):
        shred.check_hdfs_for_file('/tmp')


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
    
    
def test_write_blocks_to_zk():
    shred.log.info("Testing ZooKeeper blocklist writer")
    zk_host = shred.conf.ZOOKEEPER['HOST'] + ':' + str(shred.conf.ZOOKEEPER['PORT'])
    zk_conn = shred.connect_zk(zk_host)
    fsck_content = shred.get_fsck_output(test_files[-1])
    blocklist = shred.parse_blocks_from_fsck(fsck_content)
    shred.write_blocks_to_zk(zk_conn, blocklist)
    # TODO: Test that blocks sent to function are written as expected
