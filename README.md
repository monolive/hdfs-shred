# hdfs-shred
Proof of Concept for Secure Delete on Hadoop; to shred files deleted from HDFS for audit compliance.
  Implented in Python2.x for native compaitbility with most Linux distributions.
  Conceived as distributed collection of asynchronous workers coordinated via ZooKeeper and HDFS.
With apologies to maintainers; I couldn't resist the references to 1987 Teenage Mutant Ninja Turtles.

## Summary
Uses a three-phase approach to the secure shredding of files in HDFS, which match three operational modes for the program:

* A client (Krang); for users to specify which files are to be deleted and then shredded.  
* A worker (Foot Soldiers); to run on each Datanode, which coordinates deleting the file from HDFS while also reserving the physical sections of disk the file resided on for later shredding
* The Shredder; to run on each Datanode out-of-hours, and 'shred' the sections of disk which held the data before returning them to use in the filesystem

## Configuration


### Recommendations

* Schedule the workers on a distributed or random time slot to avoid them all hitting HDFS and ZK at once


## Operational Modes' workflows
### Client
Check that a valid file has been submitted for Shredding  
Check that HDFS Client and ZooKeeper are available  
Moves the File to /.shred directory in HDFS and creates numbered subdir to track job actions and status

### Worker
Runs every x minutes on all DataNodes  
Checks for new jobs in HDFS:/.shred  
Generates a leader election via ZK if necessary  
Leader collects block file list from Namenode, writes to numbered subdir of HDFS:/.shred for each Datanode worker, i.e HDFS:/.shred/#/DNName/blocklist, updates job status as ready for linux cp, starts polling DN status files for updates  
Workers linux-find then linux-cp local block files to a ext4:/.shred folder on the same partition, to maintain pointer to physical blocks once HDFS file is 'deleted', then update status in HDFS:/.shred/#/DNName/status file  
When all blockfiles are marked as copied, leader deletes the file from HDFS, updates job status  
Workers then update status of their job in HDFS:/.shred/#/DNName/status as ready for shredding  

### Shredder
Intended to be scheudled out-of-hours as shredding is resource intensive  
Checks for files ready for shredding and uses linux shred command to securely delete them  
Finally updates HDFS:/.shred/#/DNName/status with shredded status

## Features
* Managed via central config file.  
* Logs all activity to Syslog.  
* [TODO]Uses HDFS dir to track global job state of deletion and shredding actions.
* [TODO]Uses local Linux dir to track local block shredding actions  
* [TODO]Uses Linux cp pointer to maintain disk block ownership after HDFS delete
* Uses hadoop fsck to get file blocks. 
* [TODO]Uses HDFS Client to delete files.    
* [TODO]Uses Linux shred command to destroy disk blocks.
* [TODO]Integrates easily with Cron for scheduling


## Considered limitations
Block file locations in HDFS are dynamic; rebalancing and replication activities, for example, could move block files containing sensitive data through many locations before the data is shredded. As such, this utility only attempts to securely delete the locations of such blocks as could be reasonably found without application of extreme forensic techniques.  

Unavailable Data Nodes; it is possibly and even likely that a Data Node may be offline temporarily or permanently when a file is scheduled for shredding. Furthermore, when that Data Node comes back online, HDFS would delete any block files without shredding before this daemon could intervene to shred them. It may be that there are grounds for a hook in the HDFS process of removing these blocks which can check a list of blocks to be shreded, and this might be a suitable feature request in future. At this time we do not attempt to directly resolve this corner case.

Distributed vs Centralised process; In some architectures it may be preferable to run this process from a central MapReduce job rather than a set of distributed workers. This of course comes with its own challenges, most particularly executing remote shell commands on every DataNode. This implmentation is a distributed one because it suits the environment it is being designed for.

This implementation uses the JVM-reliant HDFS library for Python. It would be more effecient to use the HDFS3 library (http://hdfs3.readthedocs.io/en/latest/) however it has dependencies not readily available on older estates that this service is targetted at.

## Versioning Notes
0.0.1 Initial draft of logic using linux cp to maintain file control after hdfs delete  
0.0.2 Revised logic to run as async distributed service on all datanodes  
0.0.3 Revised to remove race condition of 0.0.2 by running distributed transaction via ZooKeeper  
0.0.4 Moved status and notes tracking out of ZooKeeper into HDFS to avoid melting ZK during large operations  