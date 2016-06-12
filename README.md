# hdfs-shred
Proof of Concept linux-daemon for Hadoop: Shred files deleted from HDFS for audit compliance.  

## Summary
Uses a three-phase approach to the secure shredding of files in HDFS, which match three operational modes for the program:

* A client (Krang); for users to specify which files are to be deleted and then shredded.  
* A worker (Foot Soldiers); to run on each Datanode, which coordinates deleting the file from HDFS while also reserving the physical sections of disk the file resided on for later shredding
* The Shredder; to run on each Datanode out-of-hours, and 'shred' the sections of disk which held the data before returning them to use in the filesystem

## Operational Modes
### Client
Check that a valid file has been submitted for Shredding  
Check that HDFS Client and ZooKeeper are available  
Write File as a job to ZooKeeper  

### Worker
Runs every x minutes by conf schedule on all datanodes  
Checks for new jobs in ZK  
Generates a leader election via ZK if necessary  
Checks all Datanodes are represented in the quorum  
Leader collects block file list from Namenode, writes to ZK for each Datanode worker  
Workers linux-find then linux-cp local block files to a /.shred folder on the same partition, to maintain pointer to physical blocks once HDFS file is 'deleted', then update status in ZK  
When all blockfiles are marked as copied, leader rechecks details are all valid  
If details good, leader deletes file from HDFS, skipping trash, updates status in ZK  
Workers then update status of blockfile copies in ZK ready for shredding  

### Shredder
Runs out-of-hours as shredding is resource intensive  
Checks /.shred directory on each partition and shreds any files found to a set concurrency  
Cleans up files held in /.shred directory and updates ZK with shredded status

## Features
* Managed via central config file.  
* Logs all activity to local Syslog.  
* [TODO]Uses ZooKeeper to track state of deletion and shredding actions.  
* [TODO]Uses Linux cp pointer to maintain disk block ownership after HDFS delete
* Uses hadoop fsck to get file blocks. 
* [TODO]Uses HDFS Client to delete files.    
* [TODO]Uses Linux shred command to destroy disk blocks.
* [TODO]Integrates easily with Cron for scheduling
