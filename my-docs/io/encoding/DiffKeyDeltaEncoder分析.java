DataBlockEncoding有三种类型:
PREFIX(2, new PrefixKeyDeltaEncoder()), DIFF(3, new DiffKeyDeltaEncoder()), FAST_DIFF(4, new FastDiffDeltaEncoder());

其中的数字是编码类型id，是一个short类型，用两字节表示(this.idInBytes = Bytes.toBytes(this.id))

在对每一个数据块编码是，从这里开始

	ByteBuffer org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl.encodeBufferToHFileBlockBuffer(ByteBuffer in, DataBlockEncoding algo, boolean includesMemstoreTS)

	//参数in是数据块的KeyValue列表，不含块头
	private ByteBuffer encodeBufferToHFileBlockBuffer(ByteBuffer in, DataBlockEncoding algo, boolean includesMemstoreTS) {
		ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(encodedStream);
		DataBlockEncoder encoder = algo.getEncoder();
		try {
			encodedStream.write(HFileBlock.DUMMY_HEADER);
			algo.writeIdInBytes(dataOut);
			encoder.compressKeyValues(dataOut, in, includesMemstoreTS);

			public void compressKeyValues(DataOutputStream out, ByteBuffer in, boolean includesMemstoreTS) throws IOException {
				//in=java.nio.HeapByteBuffer[pos=0 lim=1071 cap=1071]
				in.rewind();
				ByteBufferUtils.putInt(out, in.limit()); //大端编码，4个字节
					/**
					 * Put in output stream 32 bit integer (Big Endian byte order).
					 * @param out Where to put integer.
					 * @param value Value of integer.
					 * @throws IOException On stream error.
					 */
					public static void putInt(OutputStream out, final int value) throws IOException {
						//value=1071 = 0x0000042f
						//((byte) (value >>> (3 * 8))) = (byte)0x0000042f无符号右移24位 = (byte)0x00000000= 0
						//((byte) (value >>> (2 * 8))) = (byte)0x0000042f无符号右移16位 = (byte)0x00000000= 0
						//((byte) (value >>> (1 * 8))) = (byte)0x0000042f无符号右移 8位 = (byte)0x00000004= 4
						//((byte) (value >>> (0 * 8))) = (byte)0x0000042f 截取最后8位   = 0x0000002f = 2*16+15=47

						//byte[0] = 0;
						//byte[1] = 0;
						//byte[2] = 4;
						//byte[3] = 47;
						//高位的数据放在最前面(数组的小下标)，所以称为大端编码
						for (int i = Bytes.SIZEOF_INT - 1; i >= 0; --i) {
							out.write((byte) (value >>> (i * 8)));
						}
					}
				DiffCompressionState previousState = new DiffCompressionState();
				DiffCompressionState currentState = new DiffCompressionState();
				while (in.hasRemaining()) {
					compressSingleKeyValue(previousState, currentState, out, in);
					afterEncodingKeyValue(in, out, includesMemstoreTS);

					// swap previousState <-> currentState
					DiffCompressionState tmp = previousState;
					previousState = currentState;
					currentState = tmp;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(String.format("Bug in data block encoder " + "'%s', it probably requested too much data",
					algo.toString()), e);
		}
		return ByteBuffer.wrap(encodedStream.toByteArray());
	}

		private void compressSingleKeyValue(DiffCompressionState previousState, DiffCompressionState currentState,
			DataOutputStream out, ByteBuffer in) throws IOException {
		byte flag = 0;
		int kvPos = in.position();
		int keyLength = in.getInt();
		int valueLength = in.getInt();

		long timestamp;
		long diffTimestamp = 0;
		int diffTimestampFitsInBytes = 0;

		int commonPrefix;

		int timestampFitsInBytes;

		if (previousState.isFirst()) {
			currentState.readKey(in, keyLength, valueLength);
			currentState.prevOffset = kvPos;
			timestamp = currentState.timestamp;
			if (timestamp < 0) {
				flag |= FLAG_TIMESTAMP_SIGN;
				timestamp = -timestamp;
			}
			timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);

			flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
			commonPrefix = 0;

			// put column family
			in.mark();
			ByteBufferUtils.skip(in, currentState.rowLength + KeyValue.ROW_LENGTH_SIZE);

			/*
			第二部份:
			2位         rlength
			rlength位   row key 字节数组
			1位         flength
			flength位   family 字节数组
			qlength位   qualifier 字节数组 (注意: qualifier没有像row key和family那样先写qualifier的长度)
			8位         timestamp
			1位         type代码(如Put、Delete等)
			*/
			//这一句是移动
			//1位         flength
			//flength位   family 字节数组
			ByteBufferUtils.moveBufferToStream(out, in, currentState.familyLength + KeyValue.FAMILY_LENGTH_SIZE);
			in.reset();
		} else {
			// find a common prefix and skip it
			commonPrefix = ByteBufferUtils.findCommonPrefix(in, in.position(), previousState.prevOffset + KeyValue.ROW_OFFSET,
					keyLength - KeyValue.TIMESTAMP_TYPE_SIZE);
			// don't compress timestamp and type using prefix

			currentState.readKey(in, keyLength, valueLength, commonPrefix, previousState);
			currentState.prevOffset = kvPos;
			timestamp = currentState.timestamp;
			boolean negativeTimestamp = timestamp < 0;
			if (negativeTimestamp) {
				timestamp = -timestamp;
			}
			timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);

			if (keyLength == previousState.keyLength) {
				flag |= FLAG_SAME_KEY_LENGTH;
			}
			if (valueLength == previousState.valueLength) {
				flag |= FLAG_SAME_VALUE_LENGTH;
			}
			if (currentState.type == previousState.type) {
				flag |= FLAG_SAME_TYPE;
			}

			// encode timestamp
			diffTimestamp = previousState.timestamp - currentState.timestamp;
			boolean minusDiffTimestamp = diffTimestamp < 0;
			if (minusDiffTimestamp) {
				diffTimestamp = -diffTimestamp;
			}
			diffTimestampFitsInBytes = ByteBufferUtils.longFitsIn(diffTimestamp);
			if (diffTimestampFitsInBytes < timestampFitsInBytes) {
				flag |= (diffTimestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
				flag |= FLAG_TIMESTAMP_IS_DIFF;
				if (minusDiffTimestamp) {
					flag |= FLAG_TIMESTAMP_SIGN;
				}
			} else {
				flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
				if (negativeTimestamp) {
					flag |= FLAG_TIMESTAMP_SIGN;
				}
			}
		}

		out.write(flag);

		if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
			ByteBufferUtils.putCompressedInt(out, keyLength);
		}
		if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
			ByteBufferUtils.putCompressedInt(out, valueLength);
		}

		ByteBufferUtils.putCompressedInt(out, commonPrefix);
		ByteBufferUtils.skip(in, commonPrefix);

		if (previousState.isFirst() || commonPrefix < currentState.rowLength + KeyValue.ROW_LENGTH_SIZE) {
			int restRowLength = currentState.rowLength + KeyValue.ROW_LENGTH_SIZE - commonPrefix;
			ByteBufferUtils.moveBufferToStream(out, in, restRowLength);
			ByteBufferUtils.skip(in, currentState.familyLength + KeyValue.FAMILY_LENGTH_SIZE);
			ByteBufferUtils.moveBufferToStream(out, in, currentState.qualifierLength);
		} else {
			ByteBufferUtils.moveBufferToStream(out, in, keyLength - commonPrefix - KeyValue.TIMESTAMP_TYPE_SIZE);
		}

		if ((flag & FLAG_TIMESTAMP_IS_DIFF) == 0) {
			ByteBufferUtils.putLong(out, timestamp, timestampFitsInBytes);
		} else {
			ByteBufferUtils.putLong(out, diffTimestamp, diffTimestampFitsInBytes);
		}

		if ((flag & FLAG_SAME_TYPE) == 0) {
			out.write(currentState.type);
		}
		ByteBufferUtils.skip(in, KeyValue.TIMESTAMP_TYPE_SIZE);

		ByteBufferUtils.moveBufferToStream(out, in, valueLength);
	}



DIFF编码格式:
2字节: DIFF的id(3)
4字节: 数据块的总长度(大端编码Big Endian)

接下来分两种情况:
1)第一个KeyValue
1字节: familyLength长度
familyLength字节 : family名称
1字节: flag

keyLength
valueLength
commonPrefix = 0
2位         rlength
rlength位   row key 字节数组
qlength位   qualifier 字节数组
timestamp
1字节: type
vlength字节   value字节数组

memstoreTS


2) 第二个KeyValue

1字节: flag
keyLength            如果(flag & FLAG_SAME_KEY_LENGTH)==0则有keyLength
valueLength　　　　　如果(flag & FLAG_SAME_VALUE_LENGTH)==0则有valueLength
commonPrefix = 0

//如果前后两个RowKey不相等，则把剩下的RowKey: rowLength + KeyValue.ROW_LENGTH_SIZE - commonPrefix移下来
//排除family
//所以这两个要看commonPrefix的取值

rowLength + KeyValue.ROW_LENGTH_SIZE - commonPrefix个节
(2位         rlength
rlength位   row key 字节数组)

//qualifier需要完整保留
qlength位   qualifier 字节数组

//如果(flag & FLAG_SAME_KEY_LENGTH)!=0，只记录后上一个timestamp的时间差
timestamp

//如果(flag & FLAG_SAME_TYPE)==0，则有type
1字节: type
vlength字节   value字节数组

memstoreTS