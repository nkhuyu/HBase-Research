hfile.format.version 2
hbase.metrics.showTableName false

hbase.data.umask.enable false

hbase.data.umask 默认是000(在hbase-default.xml中设置)

io.file.buffer.size 4096(4k)

HDFS
------
dfs.replication 3
dfs.block.size 64 * 1024 * 1024(64M)


LocalFileSystem
------
没有dfs.replication，而是在org.apache.hadoop.fs.FileSystem.getDefaultReplication()中直接返回1
fs.local.block.size 32 * 1024 * 1024(32M)
