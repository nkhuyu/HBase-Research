1. 

DiffKeyDeltaEncoder类中有如下字段:
1	static final int FLAG_SAME_KEY_LENGTH = 1;
2	static final int FLAG_SAME_VALUE_LENGTH = 1 << 1;
4	static final int FLAG_SAME_TYPE = 1 << 2;
8	static final int FLAG_TIMESTAMP_IS_DIFF = 1 << 3;
112	static final int MASK_TIMESTAMP_LENGTH = (1 << 4) | (1 << 5) | (1 << 6);
	static final int SHIFT_TIMESTAMP_LENGTH = 4;
128	static final int FLAG_TIMESTAMP_SIGN = 1 << 7;

为什么下面这两行代码算出来的值不会与上面FLAG开头的字段冲突呢，
因为longFitsIn方法返回的值是1到8
timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);
flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;

如果用一个for循环:
	for (int timestampFitsInBytes = 1; timestampFitsInBytes <= 8; timestampFitsInBytes++)
		System.out.println((timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH);
得到如下结果:
0
16
32
48
64
80
96
112

如果timestampFitsInBytes是1,那么结果是0，所以对flag本身没影响,
其他7个数字分别是从(1 << 4)、(1 << 5)、(1 << 6)这三个因子中抽取一个、两个、三个得出的结果
抽取一个
16 = (1 << 4)
32 = (1 << 5)
64 = (1 << 6)

抽取两个
48 = (1 << 4) | (1 << 5) = 16+32
80 = (1 << 4) | (1 << 6) = 12+64
96 = (1 << 5) | (1 << 6) = 32+64

抽取三个
112 = (1 << 4) | (1 << 5) | (1 << 6)

这7个结果都不与上面的FLAG开头的字段冲突


2. 

commonPrefix = ByteBufferUtils.findCommonPrefix(in, in.position(), previousState.prevOffset + KeyValue.ROW_OFFSET,
					keyLength - KeyValue.TIMESTAMP_TYPE_SIZE);

是在同一个buffer中找，比如如果buffer是
[1,2,3,4,5,6,7,8, a,a,a,a,a,a,a, 3,3,3,3,3,3, a,a,a,a,a,a,a, 4,4,4,4,4,3]
实际上就是在找"a,a,a,a,a,a,a, 4,4,4,4,4,3"和"a,a,a,a,a,a,a, 3,3,3,3,3,3"的相同前缀，即是"a,a,a,a,a,a,a"

DiffKeyDeltaEncoder这个类是针对KeyValue的，Key的格式如下:
第一部份:
4位         keylength
4位         vlength

第二部份:
2位         rlength
rlength位   row key 字节数组
1位         flength
flength位   family 字节数组
qlength位   qualifier 字节数组 (注意: qualifier没有像row key和family那样先写qualifier的长度)
8位         timestamp
1位         type代码(如Put、Delete等)

第三部份:
vlength位   value字节数组

findCommonPrefix方法的参数就是为了找第二部份中除了最后两项外后一个KeyValue是否与前一个KeyValue相同


in变量就是buffer，in.position()就表示当前KeyValue的位置，它已经读到第二部份了，

previousState.prevOffset是上一个KeyValue的第一部份的开始位置,KeyValue.ROW_OFFSET=8,
所以previousState.prevOffset + KeyValue.ROW_OFFSET相当于前一个KeyValue的第二部份的开始位置，


对于keyLength - KeyValue.TIMESTAMP_TYPE_SIZE，keyLength代表第二部份的总字节数，
KeyValue.TIMESTAMP_TYPE_SIZE是第二部份后两项的字节数，
keyLength - KeyValue.TIMESTAMP_TYPE_SIZE的意思是说前后两个KeyValue只比较第二部份除了后两项中的其他字节


3.
void readKey(ByteBuffer in, int keyLength, int valueLength, int commonPrefix, CompressionState previousState) {
		this.keyLength = keyLength;
		this.valueLength = valueLength;

		// fill the state
		in.mark(); // mark beginning of key

		//rowKey的长度占两个字节，如果前后两个KeyValue连前两个字节都不相等，那就直接读当前KeyValue的数据了，
		if (commonPrefix < KeyValue.ROW_LENGTH_SIZE) {
			rowLength = in.getShort();
			ByteBufferUtils.skip(in, rowLength);

			familyLength = in.get();

			qualifierLength = keyLength - rowLength - familyLength - KeyValue.KEY_INFRASTRUCTURE_SIZE;
			ByteBufferUtils.skip(in, familyLength + qualifierLength);
		} else {
			rowLength = previousState.rowLength;
			//为什么这里直接赋值? 因为同一个StoreFile只会出现在一个family目录下，所以这个StoreFile中的HFile里的family肯定是一样的
			//但是qualifier可以不一样，所以下面需要keyLength - previousState.keyLength
			familyLength = previousState.familyLength;
			qualifierLength = previousState.qualifierLength + keyLength - previousState.keyLength;
			ByteBufferUtils.skip(in, (KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE) + rowLength + familyLength
					+ qualifierLength);
		}

		readTimestamp(in);

		type = in.get();

		in.reset();
	}