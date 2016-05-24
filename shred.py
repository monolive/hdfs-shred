#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Get list of blocks needed to be shredded"""

from kazoo.client import KazooClient
import sys
import re
import subprocess
import argparse
import conf
import logging
import logging.handlers

log = logging.getLogger('hdfshred')
log.setLevel(logging.INFO)
handler = logging.handlers.SysLogHandler(address='/dev/log')
formatter = logging.Formatter('%(module)s.%(funcName)s: %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
# Uncomment these two lines to echo sniffed packets to the console for debugging
# con_handler = logging.StreamHandler()
# log.addHandler(con_handler)


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Combines HDFS Delete with Linux Shred commands and audit logging to ensure file destruction"
    )
    parser.add_argument('-v', '--version', action='version', version='%(prog)s {0}'.format(conf.VERSION))
    parser.add_argument('-m', '--mode', choices=('file', 'blocks'),
                        help="Specify mode; Delete a FILE from HDFS and add to the shred queue, "
                             "or find and shred all BLOCKS in the queue on this Datanode.")
    parser.add_argument('-f', '--filename', action="store", help="Specify a filename for the --shred file mode.")
    parser.add_argument('--debug', action="store_true", help='Increase logging verbosity.')
    log.debug("Parsing commandline args")
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
    log.info("Argparsing complete, returning args to main function")
    return result


def connect_zk(host):
    """create connection to ZooKeeper"""
    zk = KazooClient(hosts=host)
    zk.start()
    if zk.state is 'CONNECTED':
        return zk
    else:
        raise "Could not connect to ZooKeeper with configuration string [{0}], resulting connection state was [{1}]"\
            .format(host, zk.state)


def run_shell_command(command):
    """Read output of shell command - line by line"""
    # http://stackoverflow.com/a/13135985
    p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
    return iter(p.stdout.readline, b'')


def check_hdfs_for_file(target):
    # TODO: Run test for file existing in given HDFS path
    # "hdfs dfs -stat " + file_to_shred
    return True


def get_fsck_output(target):
    fsck_out_iter = run_shell_command(['cat', 'sample-data.txt'])
    # "hdfs fsck " + file_to_shred + " -files -blocks -locations"
    return fsck_out_iter


def parse_blocks_from_fsck(raw_fsck):
    output = {}
    while True:
        try:
            current_line = raw_fsck.next()
            if current_line[0].isdigit():
                # TODO: Covert Block names to INTs and pack into job lots in ZK
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
    return output


def write_blocks_to_zk(zk_conn, data):
    """Write block to be deleted to zookeeper"""
    # TODO: Keep tracked of deleted block / files
    count = 1
    while count < len(data):
        zk_path = "/shred/" + data[count]
        print zk_path
        zk_conn.ensure_path(zk_path)
        zk_path = zk_path + "/" + data[0]
        zk_conn.create(zk_path, makepath=True)
        count += 1


def read_blocks_from_zk(zk_conn, dn_id):
    output = {}
    return output


def generate_shred_task_list(block_dict):
    output = {}
    return output


def shred_blocks(blocks):
    pass


def main():
    # Get invoke parameters
    args = parse_args(sys.argv[1:])
    # Test necessary connections
    # TODO: Test for HDFS connectivity
    # Test for Zookeeper connectivity
    zk_host_connection_string = conf.ZOOKEEPER['HOST'] + ':' + str(conf.ZOOKEEPER['PORT'])
    zk = connect_zk(zk_host_connection_string)

    if args.mode is 'file':
        # Test if target file exists on HDFS
        file_exists = check_hdfs_for_file(args.file_to_shred)
        if not file_exists:
            raise "File for shredding not found on HDFS: [{0}]".format(args.file_to_shred)
        # Get block information for the file we are to shred
        fsck_output = get_fsck_output(args.file_to_shred)
        # Parse HDFS fsck output into a dictionary of datanodes with lists of block IDs
        blocks_dict_out = parse_blocks_from_fsck(fsck_output)
        # Store blocks to be shredded into Zookeeper
        write_blocks_to_zk(zk, blocks_dict_out)
    elif args.mode is 'blocks':
        # Get my IP
        dn_id = "myip"
        # Get current blocks list from zk
        block_dict_in = read_blocks_from_zk(zk, dn_id)
        # Parse Block Dict into task list
        shred_task_iter = generate_shred_task_list(block_dict_in)
        # Execute Shred Processor
        shred_blocks(shred_task_iter)
    else:
        raise "Bad operating mode [{0}] detected. Please retry by specifying mode of either 'file' or 'blocks'." \
            .format(args.mode)


if __name__ == "__main__":
    main()

