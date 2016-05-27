# hdfs-shred
Client extension to ensure shredding of files deleted from HDFS.  

## Features

* Managed via central config file.  
* Logs all activity to local Syslog.  
* [TODO]Uses ZooKeeper to track state of deletion and shredding.  
* Uses hadoop fsck to get file blocks. 
* [TODO]Uses HDFS Client to delete files.    
* [TODO]Uses Linux shred command to destroy blocks as regular job.  

## Logic

### Structure

  


### nn-client, for user interaction

Get block list for a file using hdfs fsck  
Write Block ID into ZooKeeper: /shred/-IP-blkinfo-  
Delete File in HDFS, skipping trash  
Update ZK as ready for shred

### dn-worker, set in cron job on all DataNodes

Get target blocks from ZK  
Read location of block based upon their IP  
Find block location on linux FS, using find  
CP Block to a folder /shred on same mountpoint   
Update ZK  
Use shred to get rid of it  
Remove CP from /shred  
Update ZK
