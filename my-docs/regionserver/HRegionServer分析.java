HRegionServer在reportForDuty()中向HMaster报告自己启起来后，
接着调用handleReportForDutyResponse把自己挂到zookeeper的/hbase/rs节点下(是个短暂节点)，

之后HMaster就可以从/hbase/rs节点中得知有多少个HRegionServer了。


0 HRegionServer构造函数

HRegionServer构造函数, HRegionServer的RPC端口默认是60020，master的RPC端口默认是60000
HRegionServer的Jetty(InfoServer)端口默认是60030，master的Jetty(InfoServer)端口默认是60010

1 run

	1.1
	preRegistrationInitialization

	1.1.1
		initializeZooKeeper() 此方法不会创建任何节点

		. 生成ZooKeeperWatcher
		. 生成MasterAddressTracker 并等到"/hbase/master"节点有数据为止
		. 生成ClusterStatusTracker 并等到"/hbase/shutdown"节点有数据为止
		. 生成CatalogTracker 不做任何等待

	1.1.2
		initializeThreads()

		. 生成 MemStoreFlusher
		. 生成 CompactSplitThread
		. 生成 CompactionChecker
		. 生成 Leases
	
	1.1.3
	参数hbase.regionserver.nbreservationblocks默认为4，默认会预留20M(每个5M,20M = 4*5M)的内存防止OOM

	1.2
	reportForDuty

		1.2.1 getMaster()
			取出"/hbase/master"节点中的数据，构造一个master的ServerName，然后基于此生成一个HMasterRegionInterface接口的代理，
			此代理用于调用master的方法

		1.2.2 regionServerStartup

			1.2.3.1
			用rs的端口(默认是60020)、startcode(rs构造函数被调用时的时间戳)，now(当前时间)这三个参数调用master的regionServerStartup.

		1.2.3 handleReportForDutyResponse
			1.2.3.2
			regionServerStartup会返回来一个MapWritable，
			这个MapWritable有三个值:
			"hbase.regionserver.hostname.seen.by.master" = master为rs重新定义的hostname(通常跟rs的InetSocketAddress.getHostName一样)
															 rs会用它重新得到serverNameFromMasterPOV
			"fs.default.name" = "file:///"
			"hbase.rootdir"	= "file:///E:/hbase/tmp"

			这三个key的值会覆盖rs原有的conf

			1.2.3.3
			查看conf中是否有"mapred.task.id"，没有就自动设一个(格式: "hb_rs_"+serverNameFromMasterPOV)
			例如: hb_rs_localhost,60050,1323525314060

			1.2.3.4
			createMyEphemeralNode

			在zk中建立 短暂节点"/hbase/rs/localhost,60050,1323525314060"，
			也就是把当前rs的serverNameFromMasterPOV(为null的话用rs的InetSocketAddress、port、startcode构建新的ServerName)
			放到/hbase/rs节点下，"/hbase/rs/localhost,60050,1323525314060"节点没有数据。

			1.2.3.5
			设置conf中的"fs.defaultFS"为"hbase.rootdir"的值(conf之前可能没有"fs.defaultFS"属性)

			1.2.3.6
			把"hbase.rootdir"的值保存到rootDir字段，
			生成一个只读的FSTableDescriptors

			1.2.3.7
			setupWALAndReplication

				1.2.3.7.1
				得到oldLogDir = "hbase.rootdir"的值+".oldlogs"，例如: file:/E:/hbase/tmp/.oldlogs
				得到logdir		= "hbase.rootdir"的值+".logs"+serverNameFromMasterPOV，
											例如: file:/E:/hbase/tmp/.logs/localhost,60050,1323525314060

				(备注: 假设"hbase.rootdir" = "file:/E:/hbase/tmp/")

				如果logdir已存在，抛出RegionServerRunningException


				1.2.3.7.2 (TODO)
				判断是否使用Replication，默认为false，可通过"hbase.replication"参数设置

				1.2.3.7.3
				instantiateHLog
					
					1.2.3.7.3.1
					getWALActionListeners

					只有下面两个数实现了org.apache.hadoop.hbase.regionserver.wal.WALActionsListener
					org.apache.hadoop.hbase.regionserver.LogRoller
					org.apache.hadoop.hbase.replication.regionserver.Replication

					LogRoller会加入WALActionsListener列表中
					"hbase.replication"参数的值是true时，Replication也被加入


					1.2.3.7.3.2 (TODO)
					生成一个新的HLog
					在他的构造函数中会建立oldLogDir和logdir两个目录，
					prefix字段的值是serverNameFromMasterPOV经过URLEncoder.encode(prefix, "UTF8")后的值，
					然后在logdir目录中生成一个新的日志文件，
					日志文件名是prefix+当前时间戳，比如: localhost%2C60050%2C1323525314060.1323527965276

			1.2.3.8
			生成RegionServerMetrics

			1.2.3.9
			startServiceThreads

			RS_OPEN_REGION RS_OPEN_ROOT RS_OPEN_META
			RS_CLOSE_REGION RS_CLOSE_ROOT RS_CLOSE_META
			这6个并未正真启动，只是生成Executor。

			启动的线程有:
			LogRoller
			MemStoreFlusher
			CompactionChecker
			Leases
			Jetty InfoServer (可通过"/rs-status"和"/dump"这两个url来访问rs的相关信息)
			Replication(待确定 TODO)
			SplitLogWorker

		1.2.4 周期性(msgInterval默认3妙)调用doMetrics，tryRegionServerReport

			1.2.4.1
			isHealthy健康检查，只要Leases、MemStoreFlusher、LogRoller、CompactionChecker有一个线程退出，rs就停止
			
			1.2.4.2
			doMetrics

			1.2.4.3
			tryRegionServerReport
			向master汇报rs的负载HServerLoad


Map<String, HRegion> onlineRegions的可能值:
{1028785192=.META.,,1.1028785192, 5e55907939d805a9c8b224309bcd8f18=CRUDTest,,1329053412482.5e55907939d805a9c8b224309bcd8f18., 70236052=-ROOT-,,0.70236052}












