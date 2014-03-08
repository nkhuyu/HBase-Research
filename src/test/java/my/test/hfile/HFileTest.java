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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

//vm参数: -Xmx1024M -XX:MaxDirectMemorySize=1024M
public abstract class HFileTest extends TestBase {

    static final int FLAG_SAME_KEY_LENGTH = 1;
    static final int FLAG_SAME_VALUE_LENGTH = 1 << 1;
    static final int FLAG_SAME_TYPE = 1 << 2;
    static final int FLAG_TIMESTAMP_IS_DIFF = 1 << 3;
    static final int MASK_TIMESTAMP_LENGTH = (1 << 4) | (1 << 5) | (1 << 6);
    static final int SHIFT_TIMESTAMP_LENGTH = 4;
    static final int FLAG_TIMESTAMP_SIGN = 1 << 7;

    static int keyLength = 10;

    static void timestamp() {
        for (int timestampFitsInBytes = 1; timestampFitsInBytes <= 8; timestampFitsInBytes++)
            System.out.println((timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH);
    }

    static FSDataOutputStream createOutputStream(Configuration conf, FileSystem fs, Path path) throws IOException {
        FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
        return FSUtils.create(fs, path, perms);
    }

    static void putCompressedInt() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(20);
        ByteBufferUtils.putCompressedInt(out, 32767);
        ByteBufferUtils.putCompressedInt(out, 254);
        ByteBuffer bytes = ByteBuffer.wrap(out.toByteArray());
        ByteBufferUtils.readCompressedInt(bytes);
    }

    static String getKeyStr(int key) {
        return getPrettyStr(key, keyLength);
    }

    static String getPrettyStr(int num, int length) {
        String rk = Integer.toString(num);

        StringBuilder s = new StringBuilder(length);
        for (int i = 0; i < length - rk.length(); i++) {
            s.append('0');
        }

        s.append(rk);

        return s.toString();

    }

    protected Configuration conf;
    protected FileSystem fs;
    protected Path hfile;

    protected byte[] family = Bytes.toBytes("cf");
    protected byte[] qualifier = Bytes.toBytes("q1");

    protected HFileTest() throws Exception {
        conf = HBaseConfiguration.create();
        fs = FileSystem.get(conf);

        conf.setInt("hfile.index.block.max.size", 512);
        conf.setBoolean("hfile.block.index.cacheonwrite", true);

        //conf.set("hbase.offheapcache.percentage", "0.25");

        //conf.set("hbase.offheapcache.slab.proportions", "0.80,0.81,0.82");
        //conf.set("hbase.offheapcache.slab.proportions", "0.80,0");
        //conf.set("hbase.offheapcache.slab.proportions", "0.80,-1");
        //conf.set("hbase.offheapcache.slab.proportions", "0.80,2");
        //conf.set("hbase.offheapcache.slab.proportions", "0.80,0.199");
        conf.set("hbase.offheapcache.slab.proportions", "0.80,0.1");
        //conf.set("hbase.offheapcache.slab.proportions", "0.80,0.21");

        //conf.set("hfile.format.version", "0"); //非法 Invalid HFile version: 3 (expected to be between 1 and 2)
        //conf.set("hfile.format.version", "0.1"); //是2，因为"0.1"转到int时出错
        //conf.set("hfile.format.version", "3"); //非法 Invalid HFile version: 3 (expected to be between 1 and 2)
    }

    protected void deleteOldFile() throws Exception {
        //System.out.println(1024/159 + (1024%159>0 ? 1 : 0));
        //Integer.toHexString(1071);

        if (fs.exists(hfile))
            fs.delete(hfile, true);
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
        p("trailer.getNumDataIndexLevels()=" + trailer.getNumDataIndexLevels()); 

        reader.close();
        reader.close(true);
    }

    public void scan() throws IOException {
        CacheConfig cacheConfig = new CacheConfig(conf);
        HFile.Reader reader = HFile.createReader(fs, hfile, cacheConfig);
        SchemaMetrics.configureGlobally(conf);

        FixedFileTrailer trailer = reader.getTrailer();

        if (trailer.getMajorVersion() < 2)
            //hfile v1才需要，因为HFileReaderV1的构造函数没有读FileInfo，如果不调用reader.loadFileInfo()会出错
            //而hfile v2就不需要了，在HFileReaderV2的构造函数读了FileInfo。
            reader.loadFileInfo();
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
            p(rowKey);

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

        key = Bytes.toBytes(getKeyStr(1));

        byte[] reseekKey = Bytes.toBytes(getKeyStr(17));
        KeyValue reseekKV = new KeyValue(reseekKey, family, qualifier, 0L, (byte[]) null);

        byte[] key2 = new KeyValue(Bytes.toBytes(getKeyStr(16)), family, qualifier, 0L, (byte[]) null).getKey();

        KeyValue kv = new KeyValue(key, family, qualifier, 0L, (byte[]) null);
        //key = kv.getRow();
        //key = Bytes.toBytes(getKeyStr(9));

        key = kv.getKey();
        p(toS(key));
        
        seekTo(scanner, 3);
        seekTo(scanner, 9);
        seekTo(scanner, 12);

        //        scanner.seekTo();
        //        scanner.seekTo();
        //
        //        scanner.seekTo(key);
        //        scanner.seekBefore(key);
        //
        //        scanner.seekTo(key);
        //        scanner.seekTo(key);
        //        //如果当前Key>=reseekKey，那么什么都不做，
        //        //换句话说，scanner只能一直往前，不能倒退
        //        scanner.reseekTo(reseekKV.getKey());
        //
        //        scanner.seekTo(key2);
        //        scanner.reseekTo(reseekKV.getKey());

        //        do {
        //            kv = scanner.getKeyValue();
        //            if (kv == null)
        //                continue;
        //            p(toS(kv.getKey()));
        //            //System.out.println(": " + scanner.getValueString());
        //        } while (scanner.next());

        //EncodedDataBlock e = new EncodedDataBlock(DataBlockEncoding.PREFIX.getEncoder(), true);
        //      EncodedDataBlock e = new EncodedDataBlock(DataBlockEncoding.DIFF.getEncoder(), true);
        //
        //      int count = 0;
        //      //这种方式会丢失第一个KeyValue，因为next会把内部buffer的位置移动到下一个KeyValue的位置
        //      //所以要使用do while循环
        //      while (scanner.next()) {
        //          kv = scanner.getKeyValue();
        //          e.addKv(kv);
        //          System.out.print(scanner.getKeyString().substring(99));
        //          System.out.println(": " + scanner.getValueString());
        //
        //          count++;
        //          if (count > 100)
        //              break;
        //      }
        //      Iterator<KeyValue> iterator = e.getIterator();
        //      while (iterator.hasNext()) {
        //          kv = iterator.next();
        //          System.out.print(Bytes.toString(kv.getRow()).substring(99));
        //          System.out.println(": " + Bytes.toString(kv.getValue()));
        //
        //      }
    }

    protected void seekTo(HFileScanner scanner, int key) throws IOException {
        byte[] keyBytes = Bytes.toBytes(getKeyStr(key));
        KeyValue kv = new KeyValue(keyBytes, family, qualifier, 0L, (byte[]) null);
        keyBytes = kv.getKey();
        p(toS(keyBytes));
        scanner.seekTo(keyBytes);

    }
}
