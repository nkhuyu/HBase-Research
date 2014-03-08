参照hfile-v1的架构图:

总体上，按下面的顺序读取:
1. 先读Trailer
2. 再读File Info
3. 然后读Data索引和Meta索引
4. 最后读Data块和Meta块


hfile-v1的Trailer格式:
============================
8字节: block类型(BlockType.TRAILER)MAGIC ("TRABLK\"$")
8字节: fileInfoOffset
8字节: loadOnOpenDataOffset
4字节: dataIndexCount
8字节: 0值(在v2中对应uncompressedDataIndexSize)
4字节: metaIndexCount
8字节: totalUncompressedBytes
4字节: entryCount(即KeyValue个数,小于Integer.MAX_VALUE)
4字节: compressionCodec压缩算法索引号
4字节: version(值为1)
============================
总共60个字节
============================



以下是代码分析
============================

HFileReader内含HFileBlock.FSReader，HFileBlock.FSReader的实现类中才持有FSDataInputStream对像，
HFileBlock.FSReader才是真正读数据块字节流的地方。

HFileBlock.FSReaderV1中的istream和HFileReaderV1中的istream是同一个FSDataInputStream实例

HFileBlock.FSReaderV1就是用来读Data块和Meta块的，而Trailer、File Info、Data索引和Meta索引在HFileReaderV1中读，
都是让它们的istream去读。

HFileBlockIndex.BlockIndexReader解析Data索引和Meta索引


从HRegionServer = > HRegion = > Store = > StoreFile = > HFileBlock，粒度越来越小。

第1步: 调用下面的三个方法之一得到一个HFile.Reader

	1)
	org.apache.hadoop.hbase.io.hfile.HFile.createReaderWithEncoding(FileSystem fs, Path path, CacheConfig cacheConf, DataBlockEncoding preferredEncodingInCache) throws IOException


	2)
	org.apache.hadoop.hbase.io.hfile.HFile.createReader(FileSystem fs, Path path, CacheConfig cacheConf) throws IOException


	3)
	org.apache.hadoop.hbase.io.hfile.HFile.createReaderFromStream(Path path, FSDataInputStream fsdis, long size, CacheConfig cacheConf) throws IOException

	带有FSDataInputStream参数的方法3在关闭HFile.Reader时(调用close)不会关闭FSDataInputStream，由调用者自己负责关闭,
	1)和2)没有FSDataInputStream参数，当HFile.Reader关闭时会自动关闭FSDataInputStream。

	调用1)时可以指定一个首选的用于内存数据块编码，如果在写数据到硬盘时未编码，则默认使用preferredEncodingInCache，
	如果写数据到硬盘时已使用某种编码(PREFIX、DIFF、FAST_DIFF)，
	则即使指定了preferredEncodingInCache也不会用preferredEncodingInCache指定的编码。

	2)和3)默认是DataBlockEncoding.NONE，也就是说，如果在写数据到硬盘时未编码，则读到内存时也不对他们编码，
	如果写数据到硬盘时已使用某种编码则读到内存时用原本的编码。

	注: 数据块编码(DataBlockEncoding)只对HFlie V2有用，对于HFile V1读写数据都不会编码


第2步: 

	在生成HFile.Reader(HFileReaderV1或HFileReaderV2)之前先把Trailer那一部份(在文件尾)读出来生成FixedFileTrailer
	
	FixedFileTrailer.readFromStream(fsdis, size);


第3步: 
	
	得到HFile.Reader后，必须先调用它的loadFileInfo()方法

3.  生成HFileReaderV2实例，第2步得到的Trailer作为他的一个字段
	
	HFileReaderV2构造函数的处理流程:
		public HFileReaderV2(Path path, FixedFileTrailer trailer, final FSDataInputStream fsdis, final long size,
			final boolean closeIStream, final CacheConfig cacheConf, DataBlockEncoding preferredEncodingInCache)
			throws IOException {
			super(path, trailer, fsdis, size, closeIStream, cacheConf);
			trailer.expectVersion(2);

			//由fsBlockReaderV2读数据块字节流
			HFileBlock.FSReaderV2 fsBlockReaderV2 = new HFileBlock.FSReaderV2(fsdis, compressAlgo, fileSize);
			this.fsBlockReader = fsBlockReaderV2; // upcast

			// Comparator class name is stored in the trailer in version 2.
			comparator = trailer.createComparator();

			//BlockIndexReader负责解析HFileBlock.FSReaderV2读回来的字节。

			//trailer.getNumDataIndexLevels()是索引树的深度，有多少层?(不含数据块层)
			dataBlockIndexReader = new HFileBlockIndex.BlockIndexReader(comparator, trailer.getNumDataIndexLevels(), this);
			
			//只负责读Meta Index，参考hfilev2的架构图
			metaBlockIndexReader = new HFileBlockIndex.BlockIndexReader(Bytes.BYTES_RAWCOMPARATOR, 1);

			// Parse load-on-open data.

			//getLoadOnOpenDataOffset是root索引块在hfile中的相对位置，
			//trailer.getTrailerSize()是trailer块在hfile中的相对位置，
			//这个blockIter可以返回root data index和Meta index、File Info、Bloom filter metadata
			HFileBlock.BlockIterator blockIter = fsBlockReaderV2.blockRange(trailer.getLoadOnOpenDataOffset(),
					fileSize - trailer.getTrailerSize());

			// Data index. We also read statistics about the block index written after
			// the root level.
			//trailer.getDataIndexCount()是root索引块中的entry个数
			//这行代码是把root索引块读出来，然后把root索引块中的entry放到dataBlockIndexReader
			//的blockOffsets、blockKeys、blockDataSizes，如果有MID_KEY，则
			//读出MID_KEY相关的系统到midLeafBlockOffset、midLeafBlockOnDiskSize、midKeyEntry
			dataBlockIndexReader.readMultiLevelIndexRoot(blockIter.nextBlockAsStream(BlockType.ROOT_INDEX),
					trailer.getDataIndexCount());

			// Meta index.
			//读meta索引块中的entry个数，没有MID_KEY
			metaBlockIndexReader.readRootIndex(blockIter.nextBlockAsStream(BlockType.ROOT_INDEX), trailer.getMetaIndexCount());

			// File info
			//读File Info块中的entry个数，没有MID_KEY
			fileInfo = new FileInfo();
			fileInfo.readFields(blockIter.nextBlockAsStream(BlockType.FILE_INFO));
			lastKey = fileInfo.get(FileInfo.LASTKEY);
			avgKeyLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_KEY_LEN));
			avgValueLen = Bytes.toInt(fileInfo.get(FileInfo.AVG_VALUE_LEN));
			byte[] keyValueFormatVersion = fileInfo.get(HFileWriterV2.KEY_VALUE_VERSION);
			includesMemstoreTS = keyValueFormatVersion != null
					&& Bytes.toInt(keyValueFormatVersion) == HFileWriterV2.KEY_VALUE_VER_WITH_MEMSTORE;
			fsBlockReaderV2.setIncludesMemstoreTS(includesMemstoreTS);

			// Read data block encoding algorithm name from file info.
			dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo, preferredEncodingInCache);
			fsBlockReaderV2.setDataBlockEncoder(dataBlockEncoder);

			// Store all other load-on-open blocks for further consumption.
			HFileBlock b;
			//读Bloom filter metadata
			while ((b = blockIter.nextBlock()) != null) {
				loadOnOpenBlocks.add(b);
			}
		}
	
	总之，HFileReaderV2构造函数所做的事就是把Root data Index和之后的块都读出来了，
	解析完之后这些块的相关信息在HFileReaderV2的一些字段中有保存。


	要读数据块，有两种方式，第一种是用HFileBlock.FSReader的blockRange方法，传递给他一个开始位置和结束位置(不含)，
	然后就得到一个BlockIterator来遍历块，这个BlockIterator只能遍历在同一层次的块(比如"load on open"那一倍分的块)，
	不能提供遍历索引块。HFileReaderV2构造函数就是用这种方式来读出"load on open"中的块。

	第二种方式是直接调用HFileBlock.FSReader的readBlockData(long offset, long onDiskSize, int uncompressedSize, boolean pread)
	参数pread不是预读的意思，是"按位置读"，如果为true适应用随机读的场景，如果是false则适合顺序scan的场景。

	可以通过调用HFile.Reader的readBlock方法使用第二种方式


	HFileBlock.FSReader接口
		HFileBlock readBlockData(long offset, long onDiskSize, int uncompressedSize, boolean pread) throws IOException;
		BlockIterator blockRange(long startOffset, long endOffset);

		==>HFileBlock.AbstractFSReader抽像实现类(实现了blockRange方法)

			readAtOffset(byte[] dest, int destOffset, int size, boolean peekIntoNextBlock, long fileOffset, boolean pread)方法
			读字节到dest中，如果peekIntoNextBlock为true，则把下一个块的24个字节的头也读进来
				
			decompress方法
			解压

			createBufferedBoundedStream方法

			==>HFileBlock.FSReaderV1子类
				只有一个readBlockData方法

			==>HFileBlock.FSReaderV2子类
				除了有一个readBlockData方法外还有setNextBlockHeader方法，用来缓存下一个块的24个字节的头

				另外多了includesMemstoreTS和dataBlockEncoder字段，
				HFile v1中数据块中不存放MemstoreTS，而V2是需要的，
				同样HFile v1中不对数据块编码，但是V2可能需要
			

4.  调用HFile.Reader.getScanner得到HFileScanner后就可以查找(seek)数据了
	seek数据总是从HFileBlockIndex.BlockIndexReader的root索引块开始，
	在HFileReaderV2构造函数调用了readMultiLevelIndexRoot，此时root索引块中的entry已放入BlockIndexReader的blockKeys等字段中，
	接着调用BlockIndexReader.seekToDataBlock从这些root索引块中的entry中查找用户想要找的key在哪个数据块中，
	定位数据块，使用的是二分查找算法，从root到多级Intermediate-level层再到leaf层然后才到数据块层。

	protected int seekTo(byte[] key, int offset, int length, boolean rewind) throws IOException {
			HFileBlockIndex.BlockIndexReader indexReader = reader.getDataBlockIndexReader();
			HFileBlock seekToBlock = indexReader.seekToDataBlock(key, offset, length, block, cacheBlocks, pread, isCompaction);
			if (seekToBlock == null) {
				// This happens if the key e.g. falls before the beginning of the file.
				return -1;
			}
			return loadBlockAndSeekToKey(seekToBlock, rewind, key, offset, length, false);

				protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, boolean rewind, byte[] key, int offset, int length,
					boolean seekBefore) throws IOException {
					if (block == null || block.getOffset() != seekToBlock.getOffset()) {
						updateCurrBlock(seekToBlock);
					} else if (rewind) {
						blockBuffer.rewind();
					}
					return blockSeek(key, offset, length, seekBefore);


						private int blockSeek(byte[] key, int offset, int length, boolean seekBefore) {
							int klen, vlen;
							long memstoreTS = 0;
							int memstoreTSLen = 0;
							int lastKeyValueSize = -1;
							do {
								blockBuffer.mark();
								klen = blockBuffer.getInt();
								vlen = blockBuffer.getInt();
								blockBuffer.reset();
								if (this.reader.shouldIncludeMemstoreTS()) {
									try {
										int memstoreTSOffset = blockBuffer.arrayOffset() + blockBuffer.position() + KEY_VALUE_LEN_SIZE + klen
												+ vlen;
										memstoreTS = Bytes.readVLong(blockBuffer.array(), memstoreTSOffset);
										memstoreTSLen = WritableUtils.getVIntSize(memstoreTS);
									} catch (Exception e) {
										throw new RuntimeException("Error reading memstore timestamp", e);
									}
								}

								int keyOffset = blockBuffer.arrayOffset() + blockBuffer.position() + KEY_VALUE_LEN_SIZE;
								int comp = reader.getComparator().compare(key, offset, length, blockBuffer.array(), keyOffset, klen);
								
								System.out.println(Bytes.toString(key, offset, length).substring(110-10));
								System.out.println(Bytes.toString(blockBuffer.array(), keyOffset, klen).substring(110-10));
								
								if (comp == 0) {
									if (seekBefore) {
										if (lastKeyValueSize < 0) {
											throw new IllegalStateException("blockSeek with seekBefore " + "at the first key of the block: key="
													+ Bytes.toStringBinary(key) + ", blockOffset=" + block.getOffset() + ", onDiskSize="
													+ block.getOnDiskSizeWithHeader());
										}
										blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
										readKeyValueLen();
										return 1; // non exact match.
									}
									currKeyLen = klen;
									currValueLen = vlen;
									if (this.reader.shouldIncludeMemstoreTS()) {
										currMemstoreTS = memstoreTS;
										currMemstoreTSLen = memstoreTSLen;
									}
									return 0; // indicate exact match
								}

								if (comp < 0) {
									if (lastKeyValueSize > 0)
										blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
									readKeyValueLen();
									return 1;
								}

								// The size of this key/value tuple, including key/value length fields.
								lastKeyValueSize = klen + vlen + memstoreTSLen + KEY_VALUE_LEN_SIZE;
								blockBuffer.position(blockBuffer.position() + lastKeyValueSize);
							} while (blockBuffer.remaining() > 0);

							//如果Seek的key正好是块的第一个key，假设这个块是i，那么这段代码运行到这里时此块是i-1块，
							//相当于在i-1那个块的最后一个KeyValue结束，但调用next时刚好转到第i块。

							// Seek to the last key we successfully read. This will happen if this is
							// the last key/value pair in the file, in which case the following call
							// to next() has to return false.
							blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
							readKeyValueLen();
							return 1; // didn't exactly find it.
						}
				}
		}
	得到一个数据块后，再在这个数据块中查找。对应loadBlockAndSeekToKey(seekToBlock, rewind, key, offset, length, false);

	这个seekTo方法是定位key在哪个数据块，结束位置是在所查找key的前一个位置
