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

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

public class StoreFileTest extends TestBase {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        new StoreFileTest().run();
    }

    protected Configuration conf;
    protected FileSystem fs;

    public void run() throws Exception {
        conf = HBaseConfiguration.create();
        fs = FileSystem.get(conf);

        String dir = TestBase.getTestDir() + "/hfiletest/myTable/myRegion/myCF";
        Path hfilePath = new Path(dir, "MyHFileTest_Encoding2");
        Path outdir = new Path(dir);
        HFileDataBlockEncoderImpl encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.FAST_DIFF);
        StoreFile sf = new StoreFile(fs, hfilePath, conf, new CacheConfig(conf), BloomType.ROW, encoder);

        int blockSize = 1024;
        StoreFile.WriterBuilder writerBuilder = new StoreFile.WriterBuilder(conf, new CacheConfig(conf), fs, blockSize);
        //writerBuilder.withFilePath(hfilePath);
        writerBuilder.withOutputDir(outdir);
        writerBuilder.withBloomType(BloomType.ROW);
        StoreFile.Writer writer = writerBuilder.build();

        p(writer.getPath());

        @SuppressWarnings("unused")
        StoreFile.Reader reader = sf.createReader();
    }
}
