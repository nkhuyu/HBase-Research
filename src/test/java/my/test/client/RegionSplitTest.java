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

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;

public class RegionSplitTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new RegionSplitTest().run();
    }

    HBaseAdmin admin;
    AggregationClient ac;
    String tableName = "RegionSplitTest";
    byte[] cf = toB("cf");
    Configuration conf;

    public void run() throws Exception {
        deleteTable();

        createTable(tableName, "cf");
        conf = HBaseConfiguration.create();
        admin = new HBaseAdmin(conf);
        ac = new AggregationClient(conf);
        //admin.flush(tableName);

        try {
            insertRows();
            
            admin.split(toB(tableName), toB(3000));
            admin.split(toB(tableName), toB(4000));

//            T1 t1 = new T1();
//            T2 t2 = new T2();
//            t1.start();
//            t2.start();
//            t1.join();
//            t2.join();
        } finally {
            //HBaseUtils.deleteTable(tableName);
        }

    }

    void insertRows() throws Exception {
        HTable t = new HTable(conf, toB(tableName));
        for (int i = 1000; i < 5000; i++) {
            Put put = new Put(toB(i));
            put.add(cf, toB("age"), toB(30L));
            put.add(cf, toB("name"), toB("zhh-2009"));
            t.put(put);
        }

        //admin.split(toB(tableName), toB(3000));

        //admin.split(tableName);

        Get get = new Get(toB("2000"));
        get.addColumn(cf, toB("age"));
        p(t.get(get));
        t.close();
    }

    class T1 extends Thread {
        public void run() {
            try {
                admin.split(toB(tableName), toB(3000));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class T2 extends Thread {
        public void run() {
            Scan scan = new Scan();
            scan.addFamily(cf);
            scan.setStartRow(toB(1000));
            scan.setStopRow(toB(5000));
            try {
                System.out.println(ac.rowCount(toB(tableName), new LongColumnInterpreter(), scan));
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }
}
