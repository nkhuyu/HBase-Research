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
package my.test.client;

import java.util.ArrayList;
import java.util.List;

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

public class HTableTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new HTableTest().run();
    }

    public void run() throws Exception {
        tableName = "HTableTest";
        byte[] cf = toB("cf");

        //String[]familyNames = {"cf", "cf2", "cf3" };
        String[] familyNames = { "cf", "cf2" };
        HColumnDescriptor[] hcds = new HColumnDescriptor[familyNames.length];
        int i = 0;
        for (String familyName : familyNames) {
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            //hcd.setBloomFilterType(BloomType.ROW);
            hcd.setBloomFilterType(BloomType.ROWCOL);
            hcds[i++] = hcd;
        }

        //deleteTable(tableName);
        byte[][] splitKeys = { toB("30000") };
        createTable( splitKeys, hcds);

        Configuration conf = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.flush(tableName);

        try {
            HTable t = new HTable(conf, toB(tableName));
            //			HTable t = getHTable(tableName);
            //
            List<Put> puts = new ArrayList<Put>();
            Put put = new Put(toB("20000"));
            put.add(cf, toB("age"), toB(30L));
            put.add(cf, toB("name"), toB("zhh-2009"));
            puts.add(put);

            put = new Put(toB("20001"));
            put.add(cf, toB("age"), toB(30L));
            put.add(cf, toB("name"), toB("zhh-2009"));
            puts.add(put);

            put = new Put(toB("30002"));
            put.add(cf, toB("age"), toB(30L));
            put.add(cf, toB("name"), toB("zhh-2009"));
            puts.add(put);

            put = new Put(toB("30003"));
            put.add(cf, toB("age"), toB(30L));
            put.add(cf, toB("name"), toB("zhh-2009"));
            puts.add(put);

            t.put(puts);

            Get get = new Get(toB("20000"));
            //get.addFamily(cf);
            get.addColumn(cf, toB("age"));
            p(t.get(get));

            //			Increment increment = new Increment(toB("20000"));
            //			increment.addColumn(cf, toB("q1"), 20L);
            //			t.increment(increment);
            //
            //			get = new Get(toB("20000"));
            //			p(Bytes.toLong(t.get(get).getValue(cf, toB("q1"))));
            //			
            //			t.incrementColumnValue(toB("20000"), cf, toB("q1"), 30L);
            //			
            //			get = new Get(toB("20000"));
            //			p(Bytes.toLong(t.get(get).getValue(cf, toB("q1"))));
        } finally {
            //deleteTable(tableName);
        }

    }
}
