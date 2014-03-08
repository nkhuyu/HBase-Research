HFileBlockIndex只对hfile v2有用

正常流程:
先读root索引块，
对于data块相关的，调用readMultiLevelIndexRoot(间接调用readRootIndex方法) (可能存在mid-key metadata)
对于meta块相关的，调用readRootIndex方法 (不存在mid-key metadata)
然后两者都会把root索引块中的entry放入blockOffsets、blockKeys、blockDataSizes，
这样每次查找key时，调用seekToDataBlock，先对key在blockKeys中进行二分查找(rootBlockContainingKey)，
然后找出root索引块中的正确entry，得到下一级索引块的Offset和OnDiskSize(locateNonRootIndexEntry)，
再依次循环下一级索引，直到找到数据块为止。

总结调用顺序如下:
seekToDataBlock
	rootBlockContainingKey
	while {
		locateNonRootIndexEntry
			binarySearchNonRootIndex
	}
FixedFileTrailer会记住这棵索引树的层次(调用getNumDataIndexLevels)，从root到data，
索引树的层次结构是，最顶层是root，root在整个树中只有一层，然后是Intermediate层，Intermediate层可以有多层，
接着是leaf层，leaf层也只有一层，最后是data层。

为了提高性能，在读块的时候会cache住已读过的块

除了data块之外，因为每个块的entry都有一个first key，并且都有升序的，
所以要查找某个key时，每一层用二分查找算法，找完整棵树只需走完某一个分支即可，
也就是说不会出现在同一层中查找两个块，一层只会找一个块


