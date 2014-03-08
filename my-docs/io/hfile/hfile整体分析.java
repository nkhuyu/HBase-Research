1.
创建HFileWriterV2时，要么传FileSystem fs和Path path，要么传FSDataOutputStream ostream，
如果是前者会构造一个FSDataOutputStream，同时HFile内部会负责FSDataOutputStream的关闭，
在AbstractHFileWriter的构造函数中用(closeOutputStream = path != null)来表示这个FSDataOutputStream由Hfile关闭，
如果之前传的是FSDataOutputStream ostream则不需要。


hfile的数据块block的格式:

//新加checksum之前
//==================
每个block有一个24字节的头
前8个字节是表示block类型的MAGIC，对应org.apache.hadoop.hbase.io.hfile.BlockType的那些枚举常量名，
接着4个字节表示onDiskBytesWithHeader.length - HEADER_SIZE
接着4个字节表示uncompressedSizeWithoutHeader
最后8个字节表示prevOffset

//加checksum之后
//==================
每个block有一个33字节的头
前8个字节是表示block类型的MAGIC，对应org.apache.hadoop.hbase.io.hfile.BlockType的那些枚举常量名，
接着4个字节表示onDiskBytesWithHeader.length - HEADER_SIZE
接着4个字节表示uncompressedSizeWithoutHeader
接着8个字节表示prevOffset (前一个块的offset，比如，对于第一个块，那么它看到的prevOffset是-1，对于第二个块，是0)

接着1个字节表示checksumType code(默认是1: org.apache.hadoop.hbase.util.ChecksumType.CRC32)
接着4个字节表示bytesPerChecksum(默认16k，不能小于block头长度(头长度是33个字节))
最后4个字节表示onDiskDataSizeWithHeader



当不使用压缩时onDiskBytesWithHeader不包含checksums,
此时checksums放在onDiskChecksum中，
当使用压缩时checksums放在onDiskBytesWithHeader

checksums就是把onDiskBytesWithHeader中的所有字节以bytesPerChecksum个字节为单位求校验和，这个校验和用int(4字节)表示。
注意，在求校验和时，onDiskBytesWithHeader中还没有checksums


默认每个Data块的大小是64K(头(24字节)不包含在内)(可以通过HColumnDescriptor.setBlocksize设置)，
64K只是一个阀值，实际的块大小要比它大(取决于最后存入的KeyValue的大小)，
比如上次存入的KeyValue导致块大小变成63K了，但是还没到64K，那么接着存入下一个KeyValue，如果此KeyValue有5K，
那么这个块的大小就变成了63+5=68K了。

每次append到writer中的是一个KeyValue，
包括4字节的key长度和4字节的value长度，
接着再写入key的字节码和value的字节码。

同时把每个block的第一个key保存到firstKeyInBlock字段中

BlockIndex的maxChunkSize默认是128K

2. leaf索引块(或者称二级所引块)的内容如下:
   数据块总个数N(int类型,4字节)
   N个"数据块在此索引块中的相对位置"(从0开始，根据每个Entry的大小累加，每个相对位置是int类型,4字节)
   N个Entry的总字节数(int类型,4字节)
   N个Entry {
     数据块在文件中的相对位置(long类型,8字节)
     数据块的总长度(包括头)  (int类型,4字节)
	 数据块第一个KeyValue中的Key字节数组(没有vint(可变int))
   }

2. root索引块(或者称一级索引块)的内容如下:
   N个leaf索引块Entry {
     leaf索引块在文件中的相对位置(long类型,8字节)
     leaf索引块的总长度(包括头)  (int类型,4字节)
	 leaf索引块第一个Entry的Key字节数组(有vint(可变int))
   }

3. IntermediateLevel索引块
   与leaf索引块类似，只不过它的Entry在第一层IntermediateLevel是leaf索引块Entry，第二层以后是IntermediateLevel块的entry。

root索引块==>IntermediateLevel索引块==>leaf索引块==>数据块


totalNumEntries的值就是numSubEntriesAt最后一个元素的值
0-1024

firstKeyInBlock = "3, serviceName_0/hsflogcf:appName"
lastDataBlockOffset = 0
onDiskSize = 1024


dataBlockIndexWriter.addEntry(firstKeyInBlock, lastDataBlockOffset, onDiskSize);




addEntry后
BlockIndexWriter.totalNumEntries加1(totalNumEntries: 总共条目数)

BlockIndexChunk 
void add(byte[] firstKey, long blockOffset, int onDiskDataSize, long curTotalNumSubEntries) {
			// Record the offset for the secondary index
			secondaryIndexOffsetMarks.add(curTotalNonRootEntrySize);
			curTotalNonRootEntrySize += SECONDARY_INDEX_ENTRY_OVERHEAD + firstKey.length;

			//SECONDARY_INDEX_ENTRY_OVERHEAD=12是long blockOffset, int onDiskDataSize中的(long=8和int=4)所占的字节数，
			//curTotalNonRootEntrySize一开始是0，然后变成SECONDARY_INDEX_ENTRY_OVERHEAD + firstKey.length
			//secondaryIndexOffsetMarks就是记下这个0和之后的开始位置

			//curTotalRootSize与curTotalNonRootEntrySize的差别是多了WritableUtils.getVIntSize(firstKey.length),
			//WritableUtils.getVIntSize(firstKey.length)是表示firstKey.length的这个数字的位数

			curTotalRootSize += Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT + WritableUtils.getVIntSize(firstKey.length)
					+ firstKey.length;

			blockKeys.add(firstKey);
			blockOffsets.add(blockOffset);
			onDiskDataSizes.add(onDiskDataSize);

			if (curTotalNumSubEntries != -1) {
				numSubEntriesAt.add(curTotalNumSubEntries);

				// Make sure the parallel arrays are in sync.
				if (numSubEntriesAt.size() != blockKeys.size()) {
					throw new IllegalStateException("Only have key/value count " + "stats for " + numSubEntriesAt.size()
							+ " block index " + "entries out of " + blockKeys.size());
				}
			}
		}


curInlineChunk不会有totalNumEntries

只有rootChunk才有 



java.lang.Error
	at org.apache.hadoop.hbase.io.hfile.HFileWriterV2.addBloomFilter(HFileWriterV2.java:412)
	at org.apache.hadoop.hbase.regionserver.StoreFile$Writer.close(StoreFile.java:873)
	at org.apache.hadoop.hbase.regionserver.Store.internalFlushCache(Store.java:490)
	at org.apache.hadoop.hbase.regionserver.Store.flushCache(Store.java:436)
	at org.apache.hadoop.hbase.regionserver.Store.access$0(Store.java:431)
	at org.apache.hadoop.hbase.regionserver.Store$StoreFlusherImpl.flushCache(Store.java:1714)
	at org.apache.hadoop.hbase.regionserver.HRegion.internalFlushcache(HRegion.java:1159)
	at org.apache.hadoop.hbase.regionserver.HRegion.internalFlushcache(HRegion.java:1084)
	at org.apache.hadoop.hbase.regionserver.HRegion.doClose(HRegion.java:714)
	at org.apache.hadoop.hbase.regionserver.HRegion.close(HRegion.java:657)
	at org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler.process(CloseRegionHandler.java:117)
	at org.apache.hadoop.hbase.executor.EventHandler.run(EventHandler.java:167)
	at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
	at java.lang.Thread.run(Thread.java:619)




128 * 1024

128

128 B

64 * 1024 * 1024
64 * 1024

1024块

128

40
120 12

132


Key的组成:
2位         rlength
rlength位   row key 字节数组
1位         flength
flength位   family 字节数组
qlength位   qualifier 字节数组 (注意: qualifier没有像row key和family那样先写qualifier的长度)
8位         timestamp
1位         type代码(如Put、Delete等)

假设family是"f1"(长度是2),qualifier是"q1"(长度是2),row key长度是94，则整个key的总长度是:
2 + 94 + 1 + 2 + 2 + 8 + 1 = 110字节

假设每个leaf或IntermediateLevel索引块存放16个entry
并且key长度是28字节，则一个索引块的大小是512字节
  4 + 4*16 + 4 + 110*4
= 72   + 440
= 512

(x+4)*16 = 512

4+ 4*x + 4 + (12+keyLen)*x
8+ 16x + keyLen*x =512
16x + keyLen*x = 512-8
16x + 110*x = 512-8
x= 4

假设MemStore是64M，每个数据块是1k，则可以产生64 * 1024个数据块，

需要64 * 1024/16=64 * 64个leaf索引块

同时又产生64 * 64/16=64 * 4个IntermediateLevel索引块

同时再产生64 * 4/16= 16个IntermediateLevel索引块的IntermediateLevel索引块

同时再产生16/16= 1个IntermediateLevel索引块的IntermediateLevel索引块
最后产生一个root索引块(有两个IntermediateLevel索引块entry)



4+ 4*x + 4 + (12+keyLen)*x
8+ 16x + keyLen*x =512
16x + keyLen*x = 512-8
16x + 110*x = 512-8
x= 4

假设MemStore是8M，每个数据块是1k，则可以产生8 * 1024个数据块，

需要8 * 1024/16=8 * 64个leaf索引块

同时又产生8 * 64/16=32个IntermediateLevel索引块

同时再产生32/16= 2个IntermediateLevel索引块的IntermediateLevel索引块

最后产生一个root索引块(有两个IntermediateLevel索引块entry)




4+ 4*x + 4 + (12+keyLen)*x
8+ 16x + keyLen*x =512
16x + keyLen*x = 512-8
16x + 110*x = 512-8
x= 4

假设MemStore是8M，每个数据块是1k，则可以产生8 * 1024个数据块，

需要8 * 1024/4=2 * 1024个leaf索引块

同时又产生2 * 1024/4=512个IntermediateLevel索引块

同时再产生512/4= 128个IntermediateLevel索引块的IntermediateLevel索引块

同时再产生128/4= 32个IntermediateLevel索引块的IntermediateLevel索引块
同时再产生32/4= 8个IntermediateLevel索引块的IntermediateLevel索引块
同时再产生8/4= 2个IntermediateLevel索引块的IntermediateLevel索引块

最后产生一个root索引块(有两个IntermediateLevel索引块entry)






线上的算法: rowKey长度是35


Key的组成:
2位         rlength
rlength位   row key 字节数组
1位         flength
flength位   family 字节数组
qlength位   qualifier 字节数组 (注意: qualifier没有像row key和family那样先写qualifier的长度)
8位         timestamp
1位         type代码(如Put、Delete等)

假设family是"cf"(长度是2),qualifier是"sn"(长度是2),row key长度是35，则整个key的总长度是:
2 + 35 + 1 + 2 + 2 + 8 + 1 = 51字节

一个索引块的大小是128k

  4 + 4*x + 4 + 51*x
= 8   + 55x
= 128 * 1024
x = (128 * 1024 - 8)/55 = 2382.9818181818181818181818181818 = 2383

也就是一个128k的索引块能存放2383个数据库entry。


假设MemStore是64M，每个数据块是64k，则可以产生1024个数据块，

只要用一个root索引块就行了


k   v   k-b   v-b   M
4 + 4 + 126 + 18  + 7 = 152 + 7 = 159

一个KV占159字节
如果BlockSize是1024字节，1024/159 + (1024%159>0 ? 1 : 0) = 6+1 = 7 个KV
因为等于6时，还没满1024字节，所以继续写下一个KV，相关于写了7个KV后才判断大于BlockSize了，此时就生成一个HFileBlock。
所以最终一个HFileBlock的未压缩大小是: 33 + 7*159 = 1146 (其中33是块头的固定大小)

GZ + DIFF 对应的块大小是 169 到 171 字节
无压缩无编码时对应的块大小是 1150字节 (多了一个4个字节的校验和(int类型)，默认是每16K字节求一次校验和)
如果每100字节求校验和，那么将产生 1146/100 + (1146%100>0 ? 1 : 0) = 11+1 = 12个校验和，
因为1个校验和用int类型表示，所以占4个字节，所以最后一个HFileBlock存到硬盘时占用的大小是: 1146 + 12*4 = 1194字节


BlockIndexChunk

 
512 = n*(8 + 4 + 126) + 4 + 4*n + 4 
512 = n*(8 + 4 + 126) + 4*n + 8 
512 = n*(8 + 4 + 126 + 4) + 8 
n = (512-8) / (8 + 4 + 126 + 4) = 504/142 = 504/142 + (504%142>0 ? 1 : 0) = 3+1 = 4 个Block条目

一个BlockIndexChunk的未压缩大小是 33 + 4*(8 + 4 + 126 + 4) + 8 = 609字节
存到硬盘时占用的大小是: 609 + 7*4 = 637字节






HBase每个数据块的大小默认是64K，如果一个KeyValue存到数据块后占用100字节，
那么一个数据块大约能放655个KeyValue，并且这些KeyValue在数据块中按升序排列，
如果想在这个数据块查找某个Key，HBase没有使用二分查找算法，而是从头找到尾，如果数据块设得越大，KeyValue长度越小，查找性能就会变慢。