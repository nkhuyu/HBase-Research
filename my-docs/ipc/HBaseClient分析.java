HBaseClient使用的是异步转同步的方式，
并且会短暂的维护一个Connection池，每个Connection在方法调用(Call)很频繁时会被重用，
当没有Call时，Connection会被删除。

在HBaseClient.call那里，
	//param是Invocation类型
	public Writable call(Writable param, InetSocketAddress addr, Class<? extends VersionedProtocol> protocol, User ticket,
			int rpcTimeout) throws InterruptedException, IOException {
		Call call = new Call(param);
		//第一次时会启动第一个线程(假设是线程A)，并发送请求头
		Connection connection = getConnection(addr, protocol, ticket, rpcTimeout, call);

		//这里是调用call的线程，与上面的线程是并发的，异步发送参数值。
		connection.sendParam(call); // send the parameter
		boolean interrupted = false;
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		
		//等待线程A完成
		synchronized (call) {
			while (!call.done) {
				try {
					call.wait(); // wait for the result
				} catch (InterruptedException ignored) {
					// save the fact that we were interrupted
					interrupted = true;
				}
			}

			if (interrupted) {
				// set the interrupt flag now that we are done waiting
				Thread.currentThread().interrupt();
			}

			if (call.error != null) {
				if (call.error instanceof RemoteException) {
					call.error.fillInStackTrace();
					throw call.error;
				}
				// local exception
				throw wrapException(addr, call.error);
			}
			return call.value;
		}
	}

	//valueClass是HbaseObjectWritable.class
	//用来包装client端调用的返回值类型
	public HBaseClient(Class<? extends Writable> valueClass, Configuration conf, SocketFactory factory) {
		this.valueClass = valueClass;
		this.maxIdleTime = conf.getInt("hbase.ipc.client.connection.maxidletime", 10000); //10s
		this.maxRetries = conf.getInt("hbase.ipc.client.connect.max.retries", 0);
		this.failureSleep = conf.getInt("hbase.client.pause", 1000);
		this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", false);
		this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
		this.pingInterval = getPingInterval(conf);
		if (LOG.isDebugEnabled()) {
			LOG.debug("The ping interval is" + this.pingInterval + "ms.");
		}
		this.conf = conf;
		this.socketFactory = factory;
		this.clusterId = conf.get(HConstants.CLUSTER_ID, "default");
		this.connections = new PoolMap<ConnectionId, Connection>(getPoolType(conf), getPoolSize(conf));
	}
Connection的执行顺序:

1. 调用Connection的构造函数, Connection是一个Thread
2. 调用Connection.addCall增加一个Call, 每个Call有个ID，此ID也会发给Server端， 注意此时Connection还没有run
3. 调用Connection.setupIOstreams,在这一步做了这些事:
   3.1 建立Socket，连接到远程主机
   3.2 writeHeader
   3.3 启动Connection线程


4. 调用Connection.sendParam发送call中的方法参数，Server端会把ID和结果都返回来。

上面，Connection线程可能在Connection.sendParam之前运行



HBaseClient发给HBaseServer的信息格式:

头部: 按顺序如下:(对应org.apache.hadoop.hbase.ipc.HBaseClient.Connection.writeHeader())
=======================
4字节: "hrpc"(HBaseServer.HEADER)
1字节: 3 (HBaseServer.CURRENT_VERSION)
4字节: 协议类名长度N(如org.apache.hadoop.hbase.ipc.HRegionInterface类名的长度是44，加上Text.writeString后多加一个可变整数长度变成45)
       见org.apache.hadoop.hbase.ipc.ConnectionHeader
N字节: 协议类名长度+协议类名


参数体: (对应org.apache.hadoop.hbase.ipc.HBaseClient.Connection.sendParam(Call call))
=======================
4字节: 消息体长度(不含此4字节)
4字节: call id
N字节: 调用参数(值和类型)(见:org.apache.hadoop.hbase.ipc.Invocation)

写完头部和参数体后才out.flush出去。


HBaseServer发给HBaseClient的信息格式:
4字节: call id
1字节: flag (ResponseFlag: 要么是ErrorAndLength要么是Length
4字节: length 最初是0xdeadbeef，然后在bb.putInt(bufSiz)这句中重填
4字节: state
接下来根据flag的值定:
如果flag指示有错误发生，
string: errorClass
string: error堆栈(已经转成String)
如果flag指示没有错误发生，
N字节:  value

写完头部和参数体后才out.flush出去。
