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
package my.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HDFSTest {

    public static void main(String[] args) throws Exception {
        HBaseConfiguration.addDefaultResource("hdfs-site.xml");
        Configuration conf = HBaseConfiguration.create();
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("/myhbase");
        FSDataOutputStream out = fs.create(p);
        out.writeUTF("haaaa333");
        out.flush();
        out.close();

        FSDataInputStream in = fs.open(p);
        String str = in.readUTF();
        System.out.println(str);
        in.close();

        for (FileStatus fileStatus : fs.listStatus(new Path("/")))
            System.out.println(fileStatus.getPath());

        String[] args2 = { "-ls", "/" };
        org.apache.hadoop.fs.FsShell.main(args2);
    }
}
