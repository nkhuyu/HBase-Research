HFileScanner的seek或next等方法就针对单个KeyValue操作的，也就是单个列值，并不是一行。

1.
要先seekTo，然后才能做其他的，比如在调用next前要seekTo，
seekTo就是先读出来第一个块的ByteBuffer，这个ByteBuffer是没有头的，全是KeyValue，
然后事先读出第一个KeyValue的KeyLen和ValueLen，ByteBuffer的pos指向8。

2. hfile v1在读一个块出来时，会按v2的方式填充16个字节，

因为
hfile v2的block的格式是:

每个block有一个24字节的头
前8个字节是表示block类型的MAGIC，对应org.apache.hadoop.hbase.io.hfile.BlockType的那些枚举常量名，
接着4个字节表示onDiskBytesWithHeader.length - HEADER_SIZE
接着4个字节表示uncompressedSizeWithoutHeader
最后8个字节表示prevOffset

而hfile v1的block的格式只有开头的8个字节MAGIC，
v2比v1多了16字节，
所以在HFileBlock.FSReaderV1.readBlockData时
有行代码是ByteBuffer buf = ByteBuffer.allocate(uncompressedSizeWithMagic + HEADER_DELTA);
其中HEADER_DELTA是16，uncompressedSizeWithMagic就是在没压缩前块的总字节数(包括MAGIC).

hfile v1的块在解压后MAGIC的offset从16开始，然后解析得到BlockType，
当构建成一个HFileBlock时再按照v2的格式重新按v2的格式填充，此时，最前面8个字节是MAGIC。


org.apache.hadoop.hbase.io.hfile.HFileScanner
	==> org.apache.hadoop.hbase.io.hfile.AbstractHFileReader.Scanner
		
		//构造函数中初始化下面这4个变量
		protected boolean cacheBlocks;
		protected final boolean pread; //这里的pread是positional read(按位置随机读)，并不是prepare read(预读)
		protected final boolean isCompaction;
		protected final HFile.Reader reader;

		protected int currKeyLen;
		protected int currValueLen;
		protected int currMemstoreTSLen; //currMemstoreTS这个long数值占用的可变字节数，13位的时间戳占3个字节
		protected long currMemstoreTS;

		protected int blockFetches;
		protected ByteBuffer blockBuffer; //所有的KeyValue，不含块头


		==> org.apache.hadoop.hbase.io.hfile.HFileReaderV2.AbstractScannerV2

			protected HFileBlock block;

			==> org.apache.hadoop.hbase.io.hfile.HFileReaderV2.ScannerV2
				private HFileReaderV2 reader;

			==> org.apache.hadoop.hbase.io.hfile.HFileReaderV2.EncodedScannerV2
				private DataBlockEncoder.EncodedSeeker seeker = null;
				private DataBlockEncoder dataBlockEncoder = null;
				private final boolean includesMemstoreTS;


3. HFileReaderV2.ScannerV2.seekTo()

		@Override
		public boolean seekTo() throws IOException {
			if (reader == null) {
				return false;
			}

			//HFileScanner的seek或next等方法就针对单个KeyValue操作的，也就是单个列值，并不是一行。
			//在写数据时会把KeyValue的个数存放到Trailer的entryCount中，
			//如果KeyValue个数是0，那么往下查找没有意义。
			if (reader.getTrailer().getEntryCount() == 0) {
				// No data blocks.
				return false;
			}

			//第一次seekTo时，第一个数据块的Offset肯定是0
			long firstDataBlockOffset = reader.getTrailer().getFirstDataBlockOffset();

			//连续调用两次seekTo时就能走到这段代码
			if (block != null && block.getOffset() == firstDataBlockOffset) {
				blockBuffer.rewind();
				readKeyValueLen();
				return true;
			}

			block = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread, isCompaction, BlockType.DATA);

				@Override
				public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, final boolean cacheBlock, boolean pread,
						final boolean isCompaction, BlockType expectedBlockType) throws IOException {
					if (dataBlockIndexReader == null) {
						throw new IOException("Block index not loaded");
					}

					//getLoadOnOpenDataOffset是root索引块的开始位置
					if (dataBlockOffset < 0 || dataBlockOffset >= trailer.getLoadOnOpenDataOffset()) {
						throw new IOException("Requested block is out of range: " + dataBlockOffset + ", lastDataBlockOffset: "
								+ trailer.getLastDataBlockOffset());
					}
					// For any given block from any given file, synchronize reads for said
					// block.
					// Without a cache, this synchronizing is needless overhead, but really
					// the other choice is to duplicate work (which the cache would prevent you
					// from doing).

					BlockCacheKey cacheKey = new BlockCacheKey(name, dataBlockOffset,
							dataBlockEncoder.getEffectiveEncodingInCache(isCompaction), expectedBlockType);
					IdLock.Entry lockEntry = offsetLock.getLockEntry(dataBlockOffset);
					try {
						blockLoads.incrementAndGet();

						// Check cache for block. If found return.
						if (cacheConf.isBlockCacheEnabled()) {
							HFileBlock cachedBlock = (HFileBlock) cacheConf.getBlockCache().getBlock(cacheKey, cacheBlock);
							if (cachedBlock != null) {
								BlockCategory blockCategory = cachedBlock.getBlockType().getCategory();
								cacheHits.incrementAndGet();

								getSchemaMetrics().updateOnCacheHit(blockCategory, isCompaction);

								if (cachedBlock.getBlockType() == BlockType.DATA) {
									HFile.dataBlockReadCnt.incrementAndGet();
								}

								validateBlockType(cachedBlock, expectedBlockType);

								// Validate encoding type for encoded blocks. We include encoding
								// type in the cache key, and we expect it to match on a cache hit.
								if (cachedBlock.getBlockType() == BlockType.ENCODED_DATA
										&& cachedBlock.getDataBlockEncoding() != dataBlockEncoder.getEncodingInCache()) {
									throw new IOException("Cached block under key " + cacheKey + " " + "has wrong encoding: "
											+ cachedBlock.getDataBlockEncoding() + " (expected: " + dataBlockEncoder.getEncodingInCache()
											+ ")");
								}
								return cachedBlock;
							}
							// Carry on, please load.
						}

						// Load block from filesystem.
						long startTimeNs = System.nanoTime();
						HFileBlock hfileBlock = fsBlockReader.readBlockData(dataBlockOffset, onDiskBlockSize, -1, pread);
						hfileBlock = dataBlockEncoder.diskToCacheFormat(hfileBlock, isCompaction);
						validateBlockType(hfileBlock, expectedBlockType);
						passSchemaMetricsTo(hfileBlock); //把reader的SchemaMetrics信息(表名，cf名)传递给hfileBlock
						BlockCategory blockCategory = hfileBlock.getBlockType().getCategory();

						long delta = System.nanoTime() - startTimeNs;
						if (pread) {
							HFile.preadTimeNano.addAndGet(delta);
							HFile.preadOps.incrementAndGet();
						} else {
							HFile.readTimeNano.addAndGet(delta);
							HFile.readOps.incrementAndGet();
						}
						getSchemaMetrics().updateOnCacheMiss(blockCategory, isCompaction, delta);

						// Cache the block if necessary
						if (cacheBlock && cacheConf.shouldCacheBlockOnRead(hfileBlock.getBlockType().getCategory())) {
							cacheConf.getBlockCache().cacheBlock(cacheKey, hfileBlock, cacheConf.isInMemory());
						}

						if (hfileBlock.getBlockType() == BlockType.DATA) {
							HFile.dataBlockReadCnt.incrementAndGet();
						}

						return hfileBlock;
					} finally {
						offsetLock.releaseLockEntry(lockEntry);
					}
				}
			if (block.getOffset() < 0) {
				throw new IOException("Invalid block offset: " + block.getOffset());
			}
			updateCurrBlock(block);
			return true;
		}

假设root索引有4个entry，每个entry的key如下,
0010
2058
4106
6154
scanner.seekTo(6154)时会把6153也算在内，rootBlockContainingKey返回的是第3个entry

		@Override
		public int seekTo(byte[] key, int offset, int length) throws IOException {
			// Always rewind to the first key of the block, because the given key
			// might be before or after the current key.
			return seekTo(key, offset, length, true);

				protected int seekTo(byte[] key, int offset, int length, boolean rewind) throws IOException {
					HFileBlockIndex.BlockIndexReader indexReader = reader.getDataBlockIndexReader();
					HFileBlock seekToBlock = indexReader.seekToDataBlock(key, offset, length, block, cacheBlocks, pread, isCompaction);
					
						/**
						 * Return the data block which contains this key. This function will only
						 * be called when the HFile version is larger than 1.
						 *
						 * @param key the key we are looking for
						 * @param keyOffset the offset of the key in its byte array
						 * @param keyLength the length of the key
						 * @param currentBlock the current block, to avoid re-reading the same
						 *          block
						 * @return reader a basic way to load blocks
						 * @throws IOException
						 */
						public HFileBlock seekToDataBlock(final byte[] key, int keyOffset, int keyLength, HFileBlock currentBlock,
								boolean cacheBlocks, boolean pread, boolean isCompaction) throws IOException {
							int rootLevelIndex = rootBlockContainingKey(key, keyOffset, keyLength);
							
								/**
								 * Finds the root-level index block containing the given key.
								 *
								 * @param key
								 *          Key to find
								 * @return Offset of block containing <code>key</code> (between 0 and the
								 *         number of blocks - 1) or -1 if this file does not contain the
								 *         request.
								 */
								public int rootBlockContainingKey(final byte[] key, int offset, int length) {
									//find key:
									//8189

									//0000
									//2048
									//4096
									//6144
									System.out.println("find key:\r\n"+ Bytes.toString(key,offset,length));
									for(byte[] k : blockKeys)
										System.out.println(Bytes.toString(k));
									int pos = Bytes.binarySearch(blockKeys, key, offset, length, comparator);
									// pos is between -(blockKeys.length + 1) to blockKeys.length - 1, see
									// binarySearch's javadoc.

									if (pos >= 0) {
										// This means this is an exact match with an element of blockKeys.
										assert pos < blockKeys.length;
										return pos;
									}

									// Otherwise, pos = -(i + 1), where blockKeys[i - 1] < key < blockKeys[i],
									// and i is in [0, blockKeys.length]. We are returning j = i - 1 such that
									// blockKeys[j] <= key < blockKeys[j + 1]. In particular, j = -1 if
									// key < blockKeys[0], meaning the file does not contain the given key.

									int i = -pos - 1;
									assert 0 <= i && i <= blockKeys.length;
									return i - 1;
								}
							
							if (rootLevelIndex < 0 || rootLevelIndex >= blockOffsets.length) {
								return null;
							}

							// Read the next-level (intermediate or leaf) index block.
							long currentOffset = blockOffsets[rootLevelIndex];
							int currentOnDiskSize = blockDataSizes[rootLevelIndex];

							int lookupLevel = 1; // How many levels deep we are in our lookup.

							HFileBlock block;
							while (true) {

								if (currentBlock != null && currentBlock.getOffset() == currentOffset) {
									// Avoid reading the same block again, even with caching turned off.
									// This is crucial for compaction-type workload which might have
									// caching turned off. This is like a one-block cache inside the
									// scanner.
									block = currentBlock;
								} else {
									// Call HFile's caching block reader API. We always cache index
									// blocks, otherwise we might get terrible performance.
									boolean shouldCache = cacheBlocks || (lookupLevel < searchTreeLevel);
									BlockType expectedBlockType;
									if (lookupLevel < searchTreeLevel - 1) {
										expectedBlockType = BlockType.INTERMEDIATE_INDEX;
									} else if (lookupLevel == searchTreeLevel - 1) {
										expectedBlockType = BlockType.LEAF_INDEX;
									} else {
										// this also accounts for ENCODED_DATA
										expectedBlockType = BlockType.DATA;
									}
									block = cachingBlockReader.readBlock(currentOffset, currentOnDiskSize, shouldCache, pread, isCompaction,
											expectedBlockType);
								}

								if (block == null) {
									throw new IOException("Failed to read block at offset " + currentOffset + ", onDiskSize=" + currentOnDiskSize);
								}

								// Found a data block, break the loop and check our level in the tree.
								if (block.getBlockType().equals(BlockType.DATA) || block.getBlockType().equals(BlockType.ENCODED_DATA)) {
									break;
								}

								// Not a data block. This must be a leaf-level or intermediate-level
								// index block. We don't allow going deeper than searchTreeLevel.
								if (++lookupLevel > searchTreeLevel) {
									throw new IOException("Search Tree Level overflow: lookupLevel=" + lookupLevel + ", searchTreeLevel="
											+ searchTreeLevel);
								}

								// Locate the entry corresponding to the given key in the non-root
								// (leaf or intermediate-level) index block.
								ByteBuffer buffer = block.getBufferWithoutHeader();
								if (!locateNonRootIndexEntry(buffer, key, keyOffset, keyLength, comparator)) {
									throw new IOException("The key " + Bytes.toStringBinary(key, keyOffset, keyLength) + " is before the"
											+ " first key of the non-root index block " + block);
								}

								currentOffset = buffer.getLong();
								currentOnDiskSize = buffer.getInt();
							}

							if (lookupLevel != searchTreeLevel) {
								throw new IOException("Reached a data block at level " + lookupLevel + " but the number of levels is "
										+ searchTreeLevel);
							}

							return block;
						}
					
					if (seekToBlock == null) {
						// This happens if the key e.g. falls before the beginning of the file.
						return -1;
					}
					return loadBlockAndSeekToKey(seekToBlock, rewind, key, offset, length, false);
				}
		}