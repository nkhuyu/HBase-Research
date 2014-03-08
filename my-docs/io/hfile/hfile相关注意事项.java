1.
得到HFileScanner时，如果seekTo()返回false，原因可能是文件中未实际存放有KeyVlaue，
所以保险的做法是:
if(scanner.seekTo()) {
	scanner.getKey();
} else {
	....
}


2. String org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader.toString()

这个方法不能随便调用，
除非写入的数据都是符合org.apache.hadoop.hbase.KeyValue的格式的，
否则会出现java.lang.StringIndexOutOfBoundsException
特别是在单元测试时，并不是通过hbase client生成数据，直接通过如下方法写数据时可能会有问题:
void org.apache.hadoop.hbase.io.hfile.HFileWriterV2.append(byte[] key, byte[] value) throws IOException

