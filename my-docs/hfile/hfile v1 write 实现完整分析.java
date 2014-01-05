与io(或HFile)相关的操作都可以从org.apache.hadoop.hbase.io.hfile.HFile类开始

1. 通过HFile类的下面两个方法得到一个HFile.WriterFactory
	1) getWriterFactoryNoCache(Configuration)
	2) getWriterFactory(Configuration, CacheConfig)

	1)通过在Configuration中设置"hfile.block.cache.size"为0.0禁用cache，1)直接调用2)，
	得到HFile.WriterFactory后，调用它的create()方法就能得到一个HFile.Writer

	HFile.Writer是一个接口，以下是它的实现类:

	org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter
		=> org.apache.hadoop.hbase.io.hfile.HFileWriterV1
		=> org.apache.hadoop.hbase.io.hfile.HFileWriterV2


	HFile.WriterFactory是一个抽象类，对应不同的HFile.Writer有两个WriterFactory子类:
	org.apache.hadoop.hbase.io.hfile.HFileWriterV1.WriterFactoryV1
	org.apache.hadoop.hbase.io.hfile.HFileWriterV2.WriterFactoryV2


	HFile的getWriterFactory方法会根据"hfile.format.version"参数来决定使用哪个WriterFactory子类，
	"hfile.format.version"参数的有效值目前只有1和2，默认在hbase-default.xml文件中配了2，
	1代表hfile v1版本，为了兼容hbase 0.92之前的版本，2代表hfile v2版本，从hbase 0.92开始支持。

	HFile.WriterFactory类除了Configuration、CacheConfig这两个类型的字段外，
	还有下面这些字段可以使用withXXX这样的链式调用方法设置，
	比如: writerFactory.withPath(fs, hfilePath).withBlockSize(1024).withCompression(...);

		//总共11个字段，hfile v1不支持最后两个(checksumType和bytesPerChecksum)
		protected final Configuration conf;
		protected final CacheConfig cacheConf;
		//调用者可以直接传个FSDataOutputStream进来，hbase不负责关闭它，
		//或者直接提供FileSystem和Path，由hbase内部自己生成FSDataOutputStream，并负责关闭它。
		//这两种方式只能选一种
		protected FileSystem fs;
		protected Path path;
		protected FSDataOutputStream ostream;
		protected int blockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE; //64k
		protected Compression.Algorithm compression = HFile.DEFAULT_COMPRESSION_ALGORITHM; //Compression.Algorithm.NONE
		protected HFileDataBlockEncoder encoder = NoOpDataBlockEncoder.INSTANCE; //不使用数据块编码
		protected KeyComparator comparator; //用来比较KeyValue中的Key
		protected ChecksumType checksumType = HFile.DEFAULT_CHECKSUM_TYPE; //ChecksumType.CRC32
		protected int bytesPerChecksum = DEFAULT_BYTES_PER_CHECKSUM; //16k，也就是按16k算校验和


	在getWriterFactory中调用了SchemaMetrics.configureGlobally(conf); 
	这段代码是用来设置在metrics中是否加表名，可以通过"hbase.metrics.showTableName"设，
	"hbase.metrics.showTableName"在在hbase-default.xml文件中配了true


2. 通过org.apache.hadoop.hbase.io.hfile.HFileWriterV2类来写数据
	
	在上面第一步通过WriterFactory.create()就可以创新一个HFile.Writer，
	如果"hfile.format.version"是2，那么这个HFile.Writer其实就是HFileWriterV2，

	HFileWriterV2又由下面三个核心组件负责写数据:
	一个HFileBlock.Writer
	两个HFileBlockIndex.BlockIndexWriter(一个负责dataBlockIndex，另一个负责metaBlockIndex)

	@Override
	public void appendMetaBlock(String metaBlockName, Writable content) {
		byte[] key = Bytes.toBytes(metaBlockName);
		int i;

		//插入排序，按metaBlockName升序排列，
		//比如：
		/*
		writer.appendMetaBlock("CAPITAL_OF_USA", new Text("Washington, D.C."));
		writer.appendMetaBlock("CAPITAL_OF_RUSSIA", new Text("Moscow"));
		writer.appendMetaBlock("CAPITAL_OF_FRANCE", new Text("Paris"));
		
		writer.appendMetaBlock("2", new Text("Washington, D.C."));
		writer.appendMetaBlock("1", new Text("Moscow"));
		writer.appendMetaBlock("5", new Text("Paris"));
		writer.appendMetaBlock("1", new Text("Moscow"));

		排序后是:
		1
		1
		2
		5
		CAPITAL_OF_FRANCE
		CAPITAL_OF_RUSSIA
		CAPITAL_OF_USA
		*/

		for (i = 0; i < metaNames.size(); ++i) {
			// stop when the current key is greater than our own
			byte[] cur = metaNames.get(i);
			if (Bytes.BYTES_RAWCOMPARATOR.compare(cur, 0, cur.length, key, 0, key.length) > 0) {
				break;
			}
		}
		metaNames.add(i, key);
		metaData.add(i, content);
		
		for (i = 0; i < metaNames.size(); ++i) {
			byte[] cur = metaNames.get(i);
			System.out.println(Bytes.toString(cur));
		}
	}

	public byte[] getMidKeyMetadata() throws IOException {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(MID_KEY_METADATA_SIZE);
			DataOutputStream baosDos = new DataOutputStream(baos);
			//numSubEntriesAt表示总共有多少个数据块，blockKeys.size()表示有多少个leaf索引块
			long totalNumSubEntries = numSubEntriesAt.get(blockKeys.size() - 1);
			if (totalNumSubEntries == 0) {
				throw new IOException("No leaf-level entries, mid-key unavailable");
			}

			//如果totalNumSubEntries是奇数，取中间那个，因为从0开始计数，所以这个公式是对的
			//比如当totalNumSubEntries=3时，midKeySubEntry=1，因为元素索引是0,1,2，所以中间的就是1
			//如果totalNumSubEntries是偶数，此时也跟比它小1的奇数一样，比如totalNumSubEntries=4时和totalNumSubEntries=3是一样的
			long midKeySubEntry = (totalNumSubEntries - 1) / 2;
			int midKeyEntry = getEntryBySubEntry(midKeySubEntry);

[4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 128, 132, 136, 140, 144, 148, 152, 156, 160, 164, 168, 172, 176, 180, 184, 188, 192, 196, 200, 204, 208, 212, 216, 220, 224, 228, 232, 236, 240, 244, 248, 252, 256, 260, 264, 268, 272, 276, 280, 284, 288, 292, 296, 300, 304, 308, 312, 316, 320, 324, 328, 332, 336, 340, 344, 348, 352, 356, 360, 364, 368, 372, 376, 380, 384, 388, 392, 396, 400, 404, 408, 412, 416, 420, 424, 428, 432, 436, 440, 444, 448, 452, 456, 460, 464, 468, 472, 476, 480, 484, 488, 492, 496, 500, 504, 508, 512, 516, 520, 524, 528, 532, 536, 540, 544, 548, 551]