v1:

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

总共60个字节


v2:

8字节: block类型(BlockType.TRAILER)MAGIC ("TRABLK\"$")
8字节: fileInfoOffset
8字节: loadOnOpenDataOffset
4字节: dataIndexCount
8字节: uncompressedDataIndexSize(在v1中对应0值)
4字节: metaIndexCount
8字节: totalUncompressedBytes
8字节: entryCount(即KeyValue个数)
4字节: compressionCodec压缩算法索引号

4字节: numDataIndexLevels
8字节: firstDataBlockOffset
8字节: lastDataBlockOffset
128字节: comparatorClassName
4字节: version(值为2)

总共212个字节

v2与v1的差别是，v2的entryCount用8个字节表示，而v1用4个字节且不能大于Integer.MAX_VALUE,
v2比v1多了如下4项:
===============================
4字节: numDataIndexLevels
8字节: firstDataBlockOffset
8字节: lastDataBlockOffset
128字节: comparatorClassName

v2比v1总共多了152字节=4+4+8+8+128(其中第一个4是entryCount多出的4个，后面4个数字对应v2比v1多出的4项)。


v1的comparator是放在FileInfo中，作为FileInfo.COMPARATOR这个key的值

