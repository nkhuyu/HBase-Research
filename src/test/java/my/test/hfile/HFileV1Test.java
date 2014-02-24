/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package my.test.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFile.WriterFactory;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class HFileV1Test extends HFileTest {
    public static void main(String[] args) throws Exception {
        new HFileV1Test().run();
    }

    public HFileV1Test() throws Exception {
        super();
        hfile = new Path(TestBase.getTestDir() + "/hfiletest/myTable/myRegion/myCF", "MyHFileTestV1");

        conf.set("hfile.format.version", "1");
    }

    public void run() throws Exception {
        write();
        //read();
        //scan();
    }

    public void write() throws Exception {
        deleteOldFile();

        HFileDataBlockEncoderImpl encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.FAST_DIFF);
        encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.PREFIX);
        //encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.PREFIX,DataBlockEncoding.FAST_DIFF);
        //encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.DIFF);
        encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.NONE);

        //encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.NONE, DataBlockEncoding.FAST_DIFF);

        //useTableNameGlobally是static的，设过一次就不再变了
        SchemaMetrics.configureGlobally(conf); //true
        conf.set("hbase.metrics.showTableName", "false");
        SchemaMetrics.configureGlobally(conf); //false
        conf.set("hbase.metrics.showTableName", "true");
        SchemaMetrics.configureGlobally(conf); //true

        //conf.set("hfile.format.version", "3"); //1<=version<=2

        WriterFactory writerFactory = HFile.getWriterFactory(conf, new CacheConfig(conf));

        Compression.Algorithm arg = Compression.Algorithm.GZ;
        //arg = Compression.Algorithm.NONE;

        //WriterFactory writerFactory = HFile.getWriterFactoryNoCache(conf);
        writerFactory.withPath(fs, hfile).withBlockSize(1024).withCompression(arg);
        writerFactory.withDataBlockEncoder(encoder).withComparator(KeyValue.KEY_COMPARATOR);

        writerFactory.withBytesPerChecksum(100); //每100字节求校验和

        //writerFactory.withCompression("GZ"); //必须是小写
        writerFactory.withCompression("gz");

        //FSDataOutputStream ostream = createOutputStream(conf, fs, hfilePath);
        //writerFactory.withOutputStream(ostream);

        //writerFactory.withBytesPerChecksum(12);//不能小于block头长度(头长度是33个字节);

        Writer writer = writerFactory.create();

        p(writer.getPath());
        p(writer.getColumnFamilyName());

        writer.appendFileInfo(toB("key1"), toB("v1"));
        //writer.appendFileInfo(toB("hfile.k2"), toB("v2")); //key不可以用"hfile."开头
        //writer.appendFileInfo(StoreFile.DATA_BLOCK_ENCODING, DataBlockEncoding.PREFIX.getNameInBytes());
        //writer.appendFileInfo(StoreFile.DATA_BLOCK_ENCODING, DataBlockEncoding.DIFF.getNameInBytes());

        // Add in an arbitrary order. They will be sorted lexicographically by
        // the key.
        writer.appendMetaBlock("CAPITAL_OF_USA", new Text("Washington, D.C."));
        writer.appendMetaBlock("CAPITAL_OF_RUSSIA", new Text("Moscow"));
        writer.appendMetaBlock("CAPITAL_OF_FRANCE", new Text("Paris"));

        writer.appendMetaBlock("2", new Text("Washington, D.C."));
        writer.appendMetaBlock("1", new Text("Moscow"));
        writer.appendMetaBlock("5", new Text("Paris"));
        writer.appendMetaBlock("1", new Text("Moscow"));

        //writer.appendMetaBlock(Bytes.toString(StoreFile.DATA_BLOCK_ENCODING), new Text(DataBlockEncoding.PREFIX.getNameInBytes()));

        int maxKeys = 1000;
        //只有v2时，createGeneralBloomAtWrite才返回CompoundBloomFilterWriter
        //CompoundBloomFilterWriter实现了InlineBlockWriter接口
        conf.set("hfile.format.version", "2");
        Writer writer2 = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, hfile).create();
        BloomFilterWriter generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(conf, new CacheConfig(conf),
                StoreFile.BloomType.ROW, maxKeys, writer2);

        BloomFilterWriter deleteBloomFilterWriter = BloomFilterFactory.createDeleteBloomAtWrite(conf, new CacheConfig(conf),
                maxKeys, writer2);

        Configuration conf2 = new Configuration(conf);
        conf2.set("hfile.format.version", "1");
        BloomFilterWriter byteBloomFilter = BloomFilterFactory.createGeneralBloomAtWrite(conf2, new CacheConfig(conf2),
                StoreFile.BloomType.ROW, maxKeys, writer);

        System.out.println("writer.getPath()=" + writer.getPath());

        //hfile v1不支持
        //writer.addInlineBlockWriter((InlineBlockWriter) generalBloomFilterWriter);

        long totalKeyLength = 0;
        long totalValueLength = 0;

        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();

        long timestamp = new Date().getTime();

        int count = 100;
        for (int i = 1; i < count; ++i) {

            byte[] keyBytes = toB(getKeyStr(i));
            if (i == 11) {
                //keyBytes = Bytes.toBytes(getKeyStr(i) + "0");
            }
            //writer.checkKey(keyBytes, 0, (int)(Integer.MAX_VALUE+1L));

            //if(i>100)
            //	i=10;

            // A random-length random value.
            byte[] valueBytes = Bytes.toBytes("v" + getPrettyStr(i, 17));
            byte[] row = keyBytes;
            //KeyValue kv = new KeyValue(row, family, qualifier, timestamp + i, valueBytes);
            KeyValue kv = new KeyValue(row, family, qualifier, 0, valueBytes);
            kv.setMemstoreTS(timestamp + i);

            if (i == 11) {
                //kv = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("1234"), timestamp + i, valueBytes);
            }

            writer.append(kv);
            //writer.append(kv);
            //writer.append(new KeyValue(Bytes.toBytes(getKeyStr(i-1)), family, qualifier, timestamp + i, valueBytes));

            generalBloomFilterWriter.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
            byteBloomFilter.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
            if (i == 100 || i == 200)
                deleteBloomFilterWriter.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
            //writer.append(keyBytes, valueBytes);

            //if (i > 100)
            //	writer.append(keyBytes, valueBytes);

            totalKeyLength += keyBytes.length;
            totalValueLength += valueBytes.length;

            totalKeyLength += kv.getKeyLength();
            totalValueLength += kv.getValueLength();

            keys.add(keyBytes);
            values.add(valueBytes);

            if ((totalKeyLength + totalValueLength) >= 1 * 1024 * 1024)
                break;
        }
        writer.addGeneralBloomFilter(byteBloomFilter);
        //writer.addGeneralBloomFilter(generalBloomFilterWriter);

        //hfile v1不支持
        //writer.addDeleteFamilyBloomFilter(deleteBloomFilterWriter);

        //assertEquals(ENTRY_COUNT * (20 + 24), totalKeyLength + totalValueLength);

        writer.close();
    }

    public void read() throws Exception {
        SchemaMetrics.configureGlobally(conf);
        HFile.Reader reader = HFile.createReaderWithEncoding(fs, hfile, new CacheConfig(conf), DataBlockEncoding.PREFIX);

        reader.loadFileInfo(); //要先调用loadFileInfo，之后才能取到值，比如reader.getComparator()如果没有调用loadFileInfo就是null

        p(reader.getName());
        p(reader.getColumnFamilyName());
        p(reader.getComparator());

        ByteBuffer byteBuffer = reader.getMetaBlock("CAPITAL_OF_USA", true);
        p(toS(byteBuffer.array()));

        p(toS(reader.getLastKey()));
        p(toS(reader.midkey()));
        p(reader.length());
        p(reader.getEntries());
        p(toS(reader.getFirstKey()));
        p(reader.indexSize());

        p(toS(reader.getFirstRowKey()));
        p(toS(reader.getLastRowKey()));

        p(reader.getTrailer());
        p(reader.getDataBlockIndexReader());

        p(reader.getScanner(true, true));

        p(reader.getCompressionAlgorithm());
        p(reader.getGeneralBloomFilterMetadata());
        p(reader.getDeleteBloomFilterMetadata());

        p(reader.getPath());
        p(reader.getEncodingOnDisk());

        FixedFileTrailer trailer = reader.getTrailer();

        p("trailer.getMetaIndexCount()=" + trailer.getMetaIndexCount());

        reader.close();
        reader.close(true);
    }

    public void scan() throws IOException {
        HFile.Reader reader = HFile.createReader(fs, hfile, new CacheConfig(conf));
        SchemaMetrics.configureGlobally(conf);
        reader.loadFileInfo();

        FixedFileTrailer trailer = reader.getTrailer();

        HFileBlockIndex.BlockIndexReader bir = reader.getDataBlockIndexReader();
        //下面两者值是一样的
        p(bir.getRootBlockCount());
        p(trailer.getDataIndexCount());

        p(trailer.getEntryCount());

        /*总共99个KeyValue，生成15个数据块，每个数据块放7个KeyValue
         	001
        	008
        	015
        	022
        	029
        	036
        	043
        	050
        	057
        	064
        	071
        	078
        	085
        	092
        	099
         */
        String rowKey;
        for (int i = 0, count = bir.getRootBlockCount(); i < count; i++) {
            //只建立KeyValue中的Key，并生成rowKey
            rowKey = toS(KeyValue.createKeyValueFromKey(bir.getRootBlockKey(i)).getRow());
            rowKey = rowKey.substring(rowKey.length() - 3);
            //p(rowKey);

            p(toS((bir.getRootBlockKey(i))));
        }

        p();

        boolean cacheBlocks = true;
        boolean pread = true;
        boolean isCompaction = true;
        HFileScanner scanner = reader.getScanner(cacheBlocks, pread, isCompaction);

        byte[] key = Bytes.toBytes(getKeyStr(3));
        key = Bytes.toBytes(getKeyStr(8189));
        key = Bytes.toBytes(getKeyStr(2050));
        key = Bytes.toBytes(getKeyStr(2040));
        key = Bytes.toBytes(getKeyStr(112040));
        key = Bytes.toBytes(getKeyStr(2058));
        key = Bytes.toBytes(getKeyStr(6154));

        key = Bytes.toBytes(getKeyStr(0));
        key = Bytes.toBytes(getKeyStr(8));
        key = Bytes.toBytes(getKeyStr(15));
        key = Bytes.toBytes(getKeyStr(16));

        key = Bytes.toBytes(getKeyStr(18));

        key = Bytes.toBytes(getKeyStr(615400));
        key = Bytes.toBytes(getKeyStr(2));

        key = Bytes.toBytes(getKeyStr(4));

        //key = Bytes.toBytes(getKeyStr(0));

        byte[] reseekKey = Bytes.toBytes(getKeyStr(17));
        KeyValue reseekKV = new KeyValue(reseekKey, family, qualifier, 0L, (byte[]) null);

        byte[] key2 = new KeyValue(Bytes.toBytes(getKeyStr(16)), family, qualifier, 0L, (byte[]) null).getKey();

        KeyValue kv = new KeyValue(key, family, qualifier, 0L, (byte[]) null);
        //key = kv.getRow();
        //key = Bytes.toBytes(getKeyStr(9));

        key = kv.getKey();

        //scanner.seekTo();
        //scanner.seekTo();
        //scanner.seekTo(key);
        //scanner.seekBefore(key);

        scanner.seekTo(key);
        scanner.seekTo(key);
        //如果当前Key>=reseekKey，那么什么都不做，
        //换句话说，scanner只能一直往前，不能倒退
        scanner.reseekTo(reseekKV.getKey());

        scanner.seekTo(key2);
        scanner.reseekTo(reseekKV.getKey());

        do {
            kv = scanner.getKeyValue();
            p(toS(kv.getKey()));
            //System.out.println(": " + scanner.getValueString());
        } while (scanner.next());

        //EncodedDataBlock e = new EncodedDataBlock(DataBlockEncoding.PREFIX.getEncoder(), true);
        //		EncodedDataBlock e = new EncodedDataBlock(DataBlockEncoding.DIFF.getEncoder(), true);
        //
        //		int count = 0;
        //		//这种方式会丢失第一个KeyValue，因为next会把内部buffer的位置移动到下一个KeyValue的位置
        //		//所以要使用do while循环
        //		while (scanner.next()) {
        //			kv = scanner.getKeyValue();
        //			e.addKv(kv);
        //			System.out.print(scanner.getKeyString().substring(99));
        //			System.out.println(": " + scanner.getValueString());
        //
        //			count++;
        //			if (count > 100)
        //				break;
        //		}
        //		Iterator<KeyValue> iterator = e.getIterator();
        //		while (iterator.hasNext()) {
        //			kv = iterator.next();
        //			System.out.print(Bytes.toString(kv.getRow()).substring(99));
        //			System.out.println(": " + Bytes.toString(kv.getValue()));
        //
        //		}
    }
}
