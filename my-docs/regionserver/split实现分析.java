client端入口:
org.apache.hadoop.hbase.client.HBaseAdmin.split(有多个重载方法)

server端入口:
org.apache.hadoop.hbase.regionserver.HRegionServer.splitRegion(HRegionInfo)
org.apache.hadoop.hbase.regionserver.HRegionServer.splitRegion(HRegionInfo, byte[])

server端在执行split前内部会自动调用flush，
所以client端并不需要显示调用flush。