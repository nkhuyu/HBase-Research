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
package my.test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class TestBase {

    public static final Configuration sharedConf = HBaseConfiguration.create();
    private static final HTablePool tablePool = new HTablePool(sharedConf, sharedConf.getInt("hbase.htable.pool.max", 100));

    public static File getTestDir() {
        return new File(sharedConf.get("my.test.dir"));
    }

    protected String tableName;

    public TestBase() {
    }

    public TestBase(String tableName) {
        this.tableName = tableName;
    }

    public HTableInterface getHTable() {
        return (HTableInterface) tablePool.getTable(tableName);
    }

    public void createTable(String... familyNames) throws IOException {
        createTable(Compression.Algorithm.GZ, familyNames);
    }

    public void createTable(Compression.Algorithm compressionType, String... familyNames) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(sharedConf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (String familyName : familyNames) {
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            hcd.setCompressionType(compressionType);
            hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            hcd.setMaxVersions(3);
            hcd.setMinVersions(1);
            htd.addFamily(hcd);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd);
        }
        admin.close();
    }

    public void createTable(HColumnDescriptor... columnDescriptors) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(sharedConf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (HColumnDescriptor columnDescriptor : columnDescriptors) {
            if (columnDescriptor.getDataBlockEncoding() == DataBlockEncoding.NONE)
                columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            htd.addFamily(columnDescriptor);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd);
        }
        admin.close();
    }

    public void createTable(byte[][] splitKeys, HColumnDescriptor... columnDescriptors) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(sharedConf);
        HTableDescriptor htd = new HTableDescriptor(tableName);

        for (HColumnDescriptor columnDescriptor : columnDescriptors) {
            if (columnDescriptor.getDataBlockEncoding() == DataBlockEncoding.NONE)
                columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            htd.addFamily(columnDescriptor);
        }

        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd, splitKeys);
        }
        admin.close();
    }

    public void deleteTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(sharedConf);
        if (!admin.tableExists(tableName)) {
            admin.close();
            return;
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }

    public byte[] toB(String str) {
        return Bytes.toBytes(str);
    }

    public byte[] toB(long v) {
        return Bytes.toBytes(v);
    }

    public String toS(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    public void p(Object o) {
        System.out.println(o);
    }

    public void p() {
        System.out.println();
    }

    public void regions() throws Exception {
        regions(tableName);
    }

    public void regions(String tableName) throws Exception {
        HTable t = new HTable(sharedConf, tableName);
        for (Map.Entry<HRegionInfo, ServerName> e : t.getRegionLocations().entrySet()) {
            HRegionInfo info = e.getKey();
            p("info.getEncodedName()=" + info.getEncodedName());
            ServerName server = e.getValue();

            p("HRegionInfo = " + info.getRegionNameAsString());
            p("ServerName = " + server);
            p();

            //String[] args = { "-b", "-e", "-m", "-v", "-r", info.getRegionNameAsString() };
            //HFilePrettyPrinter prettyPrinter = new HFilePrettyPrinter();
            //prettyPrinter.run(args);
        }
        t.close();
    }

}
