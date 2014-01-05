1.
使用ipc时，先从org.apache.hadoop.hbase.ipc.HBaseRPC开始，

通过HBaseRPC.getServer(不是getServer(Class protocol。。。))得到一个HBaseServer，
org.apache.hadoop.hbase.master.HMaster 和
org.apache.hadoop.hbase.regionserver.HRegionServer的构造函数中就是用这个方法得到HBaseServer。

2.
HMaster不启动priority handler，默认只启动10个普通的handler线程，
而HRegionServer默认启动10个普通的handler线程和10个priority handler线程。

HMaster的普通handler线程个数可以通过参数hbase.master.handler.count设置，
如果没有设置hbase.master.handler.count那么会默认使用hbase.regionserver.handler.count，
如果hbase.regionserver.handler.count也没有设置，那么默认值是25(程序代码中的默认值)
hbase.regionserver.handler.count在hbase-default.xml文件中的默认值是10。

HRegionServer的普通handler线程个数可以通过参数hbase.regionserver.handler.count设置,
priority handler线程个数可以通过参数hbase.regionserver.metahandler.count设置，
如果没有设置，默认使用程序代码中的默认值10,hbase-default.xml文件中没有设置这个参数。

3.
IPC Reader线程默认启动10个，通过ipc.server.read.threadpool.size设置，
如果没有设置，默认使用程序代码中的默认值10,hbase-default.xml文件中没有设置这个参数。


4. RPC调用端通过getProxy和waitForProxy得到一个想要调用的接口的代理，
   RPC被调用端通过getServer得到一个HBaseServer.

   getProxy和waitForProxy的差别是waitForProxy代有重试功能，也就是如果尝试第一次getProxy失败了，再尝试更多次，
   这个重试并不是重试调用某个被代理的接口方法。