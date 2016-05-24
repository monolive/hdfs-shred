#!/usr/bin/python
# -*- coding: utf-8 -*-

from os.path import join
from glob import glob
from shlex import split as ssplit
import argparse
import pytest
import shred


test_file_size = "10"
test_file_path = "/tmp/"
test_file_name = "shred_test_file"

remove_test_data = False


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
    test_files = []
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
    if remove_test_data:
        shred.log.info("Removing Test Data")
        rmdir_cmd = ["hdfs", "dfs", "-rm", "-f", "-r", "-skipTrash", join(test_file_path, test_file_name)]
        rmdir_iter = shred.run_shell_command(rmdir_cmd)
        for line in rmdir_iter:
            shred.log.info(line)
    else:
        shred.log.info("Skipping removal of test data")


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
