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

import java.util.Date;

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFile.WriterFactory;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class HFileV2Test extends HFileTest {
    public static void main(String[] args) throws Exception {
        new HFileV2Test().run();
    }

    public HFileV2Test() throws Exception {
        super();
        hfile = new Path(TestBase.getTestDir() + "/hfiletest/myTable/myRegion/myCF", "MyHFileTestV2");

        conf.set("hfile.format.version", "2");
        //conf.set("hbase.regionserver.checksum.verify", "true");

        conf.set("hbase.data.umask.enable", "true");
        //org.apache.hadoop.fs.permission.FsPermission.FsPermission(String)的构造函数接受的参数是String，不能加0，
        //比如不能是0211而是211
        //conf.set("hbase.data.umask", "0211");
        //211代表010 001 001就是从rwx rwx rwx中除去w x x结果就是r-x rw- rw-
        //windows下会出错，不支持x
        //conf.set("hbase.data.umask", "211");
        //windows下这个也不行
        //org.apache.hadoop.fs.FileUtil.setPermission(File, FsPermission)有bug
        //f.setExecutable(group.implies(FsAction.EXECUTE), false)返回false
        conf.set("hbase.data.umask", "133");
        //这样能满足org.apache.hadoop.fs.FileUtil.setPermission(File, FsPermission)中下面这条if
        //if (group != other || NativeIO.isAvailable())
        conf.set("hbase.data.umask", "123");
        //这样就成功了，f.setExecutable(group.implies(FsAction.EXECUTE), false)返回true
        conf.set("hbase.data.umask", "122");

        conf.set(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, "0.01");
        conf.set(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, (127 * 1024) + "");
        conf.set(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, (129 * 1024) + "");

        long size = 1 * 1024 * 1024;
        conf.set("fs.local.block.size", Long.toString(size));
    }

    public void run() throws Exception {
        //write();
        read();
        //scan();
    }

    public void write() throws Exception {
        deleteOldFile();

        HFileDataBlockEncoderImpl encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.FAST_DIFF);
        //encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.PREFIX);
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
        arg = Compression.Algorithm.NONE;

        //WriterFactory writerFactory = HFile.getWriterFactoryNoCache(conf);
        writerFactory.withPath(fs, hfile).withBlockSize(1024).withCompression(arg);
        writerFactory.withDataBlockEncoder(encoder).withComparator(KeyValue.KEY_COMPARATOR);

        writerFactory.withBytesPerChecksum(100); //每100字节求校验和

        //writerFactory.withCompression("GZ"); //必须是小写
        //writerFactory.withCompression("gz");

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

        // Add in an arbitrary order. They will be sorted lexicographically by the key.
        //按metaNames中的元素进行升序排列
        writer.appendMetaBlock("CAPITAL_OF_USA", new Text("Washington, D.C."));
        writer.appendMetaBlock("CAPITAL_OF_RUSSIA", new Text("Moscow"));
        writer.appendMetaBlock("CAPITAL_OF_FRANCE", new Text("Paris"));

        writer.appendMetaBlock("2", new Text("Washington, D.C."));
        writer.appendMetaBlock("1", new Text("Moscow"));
        writer.appendMetaBlock("5", new Text("Paris"));
        writer.appendMetaBlock("1", new Text("Moscow"));

        //writer.appendMetaBlock(Bytes.toString(StoreFile.DATA_BLOCK_ENCODING), new Text(DataBlockEncoding.PREFIX.getNameInBytes()));

        int maxKeys = 10;
        //HFile V2返回的BloomFilterWriter是CompoundBloomFilterWriter，
        //CompoundBloomFilterWriter也实现了InlineBlockWriter接口
        BloomFilterWriter generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(conf, new CacheConfig(conf),
                StoreFile.BloomType.ROW, maxKeys, writer);

        BloomFilterWriter generalBloomFilterWriterROWCOL = BloomFilterFactory.createGeneralBloomAtWrite(conf, new CacheConfig(
                conf), StoreFile.BloomType.ROWCOL, maxKeys, writer);

        BloomFilterWriter deleteBloomFilterWriter = BloomFilterFactory.createDeleteBloomAtWrite(conf, new CacheConfig(conf),
                maxKeys, writer);

        //在BloomFilterFactory的createGeneralBloomAtWrite和createDeleteBloomAtWrite中已调用过addInlineBlockWriter，
        //所以不需要再调用一次
        //writer.addInlineBlockWriter((InlineBlockWriter) generalBloomFilterWriter);
        //writer.addInlineBlockWriter((InlineBlockWriter) deleteBloomFilterWriter);

        Configuration conf2 = new Configuration(conf);
        conf2.set("hfile.format.version", "1");
        BloomFilterWriter byteBloomFilter = BloomFilterFactory.createGeneralBloomAtWrite(conf2, new CacheConfig(conf2),
                StoreFile.BloomType.ROW, maxKeys, writer);

        long timestamp = new Date().getTime();
        int count = 10000;

        for (int i = 1; i < count; ++i) {
            byte[] keyBytes = toB(getPrettyStr(i, 10));
            byte[] valueBytes = Bytes.toBytes("v" + getPrettyStr(i, 214));
            //KeyValue kv = new KeyValue(row, family, qualifier, timestamp + i, valueBytes);
            KeyValue kv = new KeyValue(keyBytes, family, qualifier, 0, valueBytes);
            kv.setMemstoreTS(timestamp + i);

            writer.append(kv);
            //writer.append(kv);
            //writer.append(new KeyValue(Bytes.toBytes(getKeyStr(i-1)), family, qualifier, timestamp + i, valueBytes));

            generalBloomFilterWriter.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
            generalBloomFilterWriterROWCOL.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
            byteBloomFilter.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
            if (i == 100 || i == 200)
                deleteBloomFilterWriter.add(kv.getBuffer(), kv.getOffset(), kv.getLength());
        }
        writer.addGeneralBloomFilter(byteBloomFilter);
        writer.addGeneralBloomFilter(generalBloomFilterWriter);
        writer.addDeleteFamilyBloomFilter(deleteBloomFilterWriter);

        writer.close();
    }

    @Override
    public void read() throws Exception {
        super.read();
    }
}
