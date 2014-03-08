1. 运行org.apache.hadoop.hbase.io.hfile.HFile.main(String[] args)
会调用org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter

先用下面的代码查看一个表有哪些分区:
		HTable t = new HTable(config, "mytable");
		for (Map.Entry<HRegionInfo, ServerName> e : t.getRegionLocations().entrySet()) {
			HRegionInfo info = e.getKey();
			ServerName server = e.getValue();

			System.out.println("HRegionInfo = " + info);
			System.out.println("ServerName = " + server);
		}
然后运行HFile.main时指定一个-r 的参数:

-v -r mytable,,1322184198804.07344a9dab506cd7b2d7c213615d659e.


-v 表示输出详细的信息


-v -b -s -m -r hsf_habo_log2,,1322544989664.8134d531358b75df8fc0dfb6006dd8f1. -f file:/E:/hbase/....