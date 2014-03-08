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
package my.test.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.Assert.assertTrue;

public class StoreFileTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new StoreFileTest().run();
    }

    protected Configuration conf;
    protected FileSystem fs;

    public void run() throws Exception {

        conf = HBaseConfiguration.create();
        fs = FileSystem.get(conf);

        //testStoreFileScanner();

        //testMultipleTimestamps();

        testRequestSeek();
    }

    public void testStoreFileScanner() throws Exception {

        String dir = TestBase.getTestDir() + "/hfiletest/myTable/myRegion/myCF";
        Path hfilePath = new Path(dir, "MyHFileTestV2");
        Path outdir = new Path(dir);
        HFileDataBlockEncoderImpl encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.FAST_DIFF);
        encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.NONE);
        StoreFile sf = new StoreFile(fs, hfilePath, conf, new CacheConfig(conf), BloomType.NONE, encoder);

        int blockSize = 1024;
        StoreFile.WriterBuilder writerBuilder = new StoreFile.WriterBuilder(conf, new CacheConfig(conf), fs, blockSize);
        //writerBuilder.withFilePath(hfilePath);
        writerBuilder.withOutputDir(outdir);
        writerBuilder.withBloomType(BloomType.ROW);
        StoreFile.Writer writer = writerBuilder.build();

        p(writer.getPath());

        StoreFile.Reader reader = sf.createReader();

        boolean cacheBlocks = true;
        boolean pread = true;
        KeyValueScanner kvs = reader.getStoreFileScanner(cacheBlocks, pread);
        kvs.seek(null);
        KeyValue kv = null;
        while (((kv = kvs.next()) != null))
            p(kv);
    }

    List<KeyValue> getKeyValueSet(long[] timestamps, int numRows, byte[] qualifier, byte[] family) {
        List<KeyValue> kvList = new ArrayList<KeyValue>();
        for (int i = 1; i <= numRows; i++) {
            byte[] b = Bytes.toBytes(i);
            for (long timestamp : timestamps) {
                kvList.add(new KeyValue(b, family, qualifier, timestamp, b));
            }
        }
        return kvList;
    }

    public void testMultipleTimestamps() throws IOException {
        byte[] family = Bytes.toBytes("familyname");
        byte[] qualifier = Bytes.toBytes("qualifier");
        int numRows = 10;
        long[] timestamps = new long[] { 20, 10, 5, 1 };
        Scan scan = new Scan();

        String dir = TestBase.getTestDir() + "/hfiletest/myTable/myRegion/myCF";
        Path hfilePath = new Path(dir, "StoreFileTest");
        Path outdir = new Path(dir);
        HFileDataBlockEncoderImpl encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.FAST_DIFF);
        encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.NONE);
        StoreFile sf = new StoreFile(fs, hfilePath, conf, new CacheConfig(conf), BloomType.NONE, encoder);

        int blockSize = 1024;
        StoreFile.WriterBuilder writerBuilder = new StoreFile.WriterBuilder(conf, new CacheConfig(conf), fs, blockSize);
        writerBuilder.withFilePath(hfilePath);
        //writerBuilder.withOutputDir(outdir);
        writerBuilder.withBloomType(BloomType.ROW);
        StoreFile.Writer writer = writerBuilder.build();

        List<KeyValue> kvList = getKeyValueSet(timestamps, numRows, family, qualifier);

        for (KeyValue kv : kvList) {
            writer.append(kv);
        }
        writer.appendMetadata(0, false);
        writer.close();

        sf = new StoreFile(fs, hfilePath, conf, new CacheConfig(conf), BloomType.NONE, encoder);
        StoreFile.Reader reader = sf.createReader();
        StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
        TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        columns.add(qualifier);

        scan.setTimeRange(20, 100);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        scan.setTimeRange(1, 2);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        scan.setTimeRange(8, 10);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        scan.setTimeRange(7, 50);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        // This test relies on the timestamp range optimization
        scan.setTimeRange(27, 50);
        assertTrue(!scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));
    }

    public void testRequestSeek() throws IOException {
        byte[] family = Bytes.toBytes("familyname");
        byte[] qualifier = Bytes.toBytes("qualifier");
        int numRows = 10;
        long[] timestamps = new long[] { 20, 10, 5, 1 };
        Scan scan = new Scan();

        String dir = TestBase.getTestDir() + "/hfiletest/myTable/myRegion/myCF";
        Path hfilePath = new Path(dir, "StoreFileTest");
        Path outdir = new Path(dir);
        HFileDataBlockEncoderImpl encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.FAST_DIFF);
        encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.NONE);
        StoreFile sf = new StoreFile(fs, hfilePath, conf, new CacheConfig(conf), BloomType.ROWCOL, encoder);

        //写的时候不需要StoreFile的实例
        int blockSize = 1024;
        long maxKeyCount = 10000;
        StoreFile.WriterBuilder writerBuilder = new StoreFile.WriterBuilder(conf, new CacheConfig(conf), fs, blockSize);
        writerBuilder.withFilePath(hfilePath);
        //writerBuilder.withOutputDir(outdir);
        writerBuilder.withBloomType(BloomType.ROWCOL);
        writerBuilder.withMaxKeyCount(maxKeyCount);
        StoreFile.Writer writer = writerBuilder.build();

        List<KeyValue> kvList = getKeyValueSet(timestamps, numRows, family, qualifier);

        for (KeyValue kv : kvList) {
            writer.append(kv);
        }
        writer.appendMetadata(0, false);
        writer.close();

        sf = new StoreFile(fs, hfilePath, conf, new CacheConfig(conf), BloomType.ROWCOL, encoder);
        StoreFile.Reader reader = sf.createReader();
        StoreFileScanner scanner = reader.getStoreFileScanner(false, false);

        byte[] b = Bytes.toBytes(2);
        KeyValue kv = new KeyValue(b, family, qualifier, 10, b);
        boolean forward = true;
        boolean useBloom = true;

        scanner.requestSeek(kv, forward, useBloom);
        TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        columns.add(qualifier);

        scan.setTimeRange(20, 100);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        scan.setTimeRange(1, 2);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        scan.setTimeRange(8, 10);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        scan.setTimeRange(7, 50);
        assertTrue(scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));

        // This test relies on the timestamp range optimization
        scan.setTimeRange(27, 50);
        assertTrue(!scanner.shouldUseScanner(scan, columns, Long.MIN_VALUE));
    }
}
