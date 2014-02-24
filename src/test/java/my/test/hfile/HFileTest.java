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
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
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

    static int keyLength = 110;

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
        System.out.println("reader.getEncodingOnDisk()=" + reader.getEncodingOnDisk());

        ByteBuffer byteBuffer = reader.getMetaBlock("CAPITAL_OF_USA", true);

        System.out.println("byteBuffer=" + Bytes.toString(byteBuffer.array()));

        FixedFileTrailer trailer = reader.getTrailer();

        System.out.println("trailer.getMetaIndexCount()=" + trailer.getMetaIndexCount());
        //HFileBlockIndex.BlockIndexReader blockIndexReader = reader.getDataBlockIndexReader();

        //blockIndexReader.

        reader.close();
    }
}
