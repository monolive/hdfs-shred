# hdfs-shred
Use shred Linux command to delete block

Logic

NN
===
Get block list for a file using hdfs fsck 

Write Block ID inside /shred/IP



DN 
===
Read location of block based upon their IP 

Find block location on FS, using find ?

Move Block to a folder /shred on same mountpoint 

Update ZK

Use shred to get ride of it 
