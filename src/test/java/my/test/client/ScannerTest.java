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
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;

public class ScannerTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new ScannerTest().run();
    }

    Configuration conf;

    public void run() throws Exception {
        conf = HBaseConfiguration.create();
        tableName = "ScannerTest";
        byte[] cf = toB("cf");
        createTable("cf");

        //createTable("cf", "cf2", "cf3");

        createTable("cf1", "cf2");

        putRows();
        run2();
        try {
            //			HTable t = getHTable(tableName);
            //
            //			Put put = new Put(toB("20000"));
            //			put.add(cf, toB("q1"), toB(10L));
            //			t.put(put);
            //
            //			Get get = new Get(toB("20000"));
            //			p(t.get(get));
            //
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

    public static final String TABLE_NAME = "ScannerTest";
    public static final String FAMILY1 = "cf1";
    public static final String FAMILY2 = "cf2";

    public void putRows() throws Exception {
        //        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY1);
        //        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        //        hcd.setCompressionType(Compression.Algorithm.GZ);
        //        hcd.setBlocksize(1024);
        //        hcd.setBloomFilterType(org.apache.hadoop.hbase.regionserver.StoreFile.BloomType.ROW);
        //
        //        hcd.setMaxVersions(1000);
        //        hcd.setMinVersions(1);
        //        byte[][] splitKeys = new byte[][] { toBytes("2000"), toBytes("3000"), toBytes("4000") };
        //        createTable(TABLE_NAME, splitKeys, hcd);
        //        

        List<Put> rows = new ArrayList<Put>();
        for (int i = 1000; i < 4000; i++) {
            long ts = System.currentTimeMillis();
            Put put = new Put(toB("" + i));
            put.add(toB(FAMILY1), toB("q"), ts, toB("" + i));
            put.add(toB(FAMILY1), toB("q2"), ts, toB("" + i));
            put.add(toB(FAMILY1), toB("q3"), ts, toB("" + i));
            put.add(toB(FAMILY1), toB("q4"), ts, toB("" + i));
            rows.add(put);
        }

        HTable t = new HTable(HBaseConfiguration.create(), TABLE_NAME);
        t.put(rows);
    }

    public void run0() throws Exception {
        //deleteTable(TABLE_NAME);
        //put1();
        //run3();
        //run4();
        try {
            //aggregationClient();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    //在org.apache.hadoop.hbase.regionserver.HRegionServer.openScanner(byte[], Scan)打断点scan.getCaching()==2000
    public void run2() throws Exception {

        //HTable t = new HTable(HBaseConfiguration.create(), TABLE_NAME);
        HTable t = new HTable(conf, TABLE_NAME);

        ResultScanner rs;// = t.getScanner(toB(FAMILY1));

        Scan scan = new Scan();
        //scan.setMaxResultSize(100);

        //scan.addColumn(toB(FAMILY1), toB("q"));

        //scan.addFamily(toB(FAMILY1));

        scan.addColumn(toB(FAMILY1), toB("q2"));
        scan.addColumn(toB(FAMILY1), toB("q3"));

        //scan.setRaw(true);

        //scan.setStartRow(toB("1950"));
        //scan.setStopRow(toB("2050"));

        Configuration conf = HBaseConfiguration.create();
        conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");
        conf.set(ScannerCallable.LOG_SCANNER_LATENCY_CUTOFF, "300");
        //conf.setLong("hbase.regionserver.lease.period", 10000);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, toB("1"));

        //conf.set(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, "1000");

        scan.setMaxVersions(3);

        scan.setCaching(100);
        scan.setCaching(1050);
        scan.setCaching(2000);
        //scan.setCaching(4);

        //scan.setCaching(2);

        //scan.setBatch(4);
        scan.setBatch(2);
        //scan.setBatch(5);

        Delete d = new Delete(toB("2001"));
        //d.deleteFamily(cf);
        //t.delete(d);

        //scan.setBatch(1); //batch的意思是返回的一行有多少个字段，假设一行有4个字段，batch设为1，那么一次next只返回1个字段

        scan.setStartRow(toB("2000"));
        scan.setStopRow(toB("2005"));
        scan.setStopRow(toB("2000"));

        //scan.setStopRow(toB("2000"));

        rs = new ClientScanner(conf, scan, toB(TABLE_NAME));

        //Result[] result = rs.next(1050);
        //p("length=" + result.length);
        //rs.next(2000);

        //rs.next(2000);
        int count = 0;
        for (Result r : rs) {
            p(r);
            count++;
        }
        rs.close();

        p("count=" + count); //4个字段且batch是1时，count是8，因为从"2000"到"2002"有两条记录，共8个值，一次next返回一个值

        //        AggregationClient ac = new AggregationClient(conf);
        //
        //        //scan = new Scan();
        //
        //        scan.addColumn(toB(FAMILY1), toB("q2"));
        //        scan.addColumn(toB(FAMILY1), toB("q3"));
        //        scan.addColumn(toB(FAMILY1), toB("q0"));
        //        scan.addColumn(toB(FAMILY1), toB("q1"));
        //
        //        //scan.setFilter(new SkipFilter());
        //        try {
        //            p("count=" + ac.rowCount(toB(TABLE_NAME), new LongColumnInterpreter(), scan));
        //        } catch (Throwable e) {
        //            e.printStackTrace();
        //        }
    }

    public void run3() throws Exception {
        //deleteTable(TABLE_NAME);
        //put1();

        //HTable t = new HTable(HBaseConfiguration.create(), TABLE_NAME);

        ResultScanner rs;// = t.getScanner(toB(FAMILY1));

        Scan scan = new Scan();
        //scan.setMaxResultSize(100);

        Configuration conf = HBaseConfiguration.create();
        conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");
        conf.set(ScannerCallable.LOG_SCANNER_LATENCY_CUTOFF, "300");
        //conf.setLong("hbase.regionserver.lease.period", 10000);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, toB("1"));

        scan.setCaching(1000);
        scan.setCaching(1050);

        rs = new ClientScanner(conf, scan, toB(TABLE_NAME));
        //org.apache.hadoop.hbase.client.ScannerCallable.flag=true;
        rs.next(1050);
        int count = 0;
        for (Result r : rs) {
            p(r);
            count++;
        }
        p("count=" + count);
        rs.close();
    }

    public void run4() throws Exception {
        //deleteTable(TABLE_NAME);
        //put1();

        //HTable t = new HTable(HBaseConfiguration.create(), TABLE_NAME);

        ResultScanner rs;// = t.getScanner(toB(FAMILY1));

        Scan scan = new Scan();
        //scan.setMaxResultSize(300);

        Configuration conf = HBaseConfiguration.create();
        conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");
        conf.set(ScannerCallable.LOG_SCANNER_LATENCY_CUTOFF, "300");
        //conf.setLong("hbase.regionserver.lease.period", 10000);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, toB("1"));

        scan.setCaching(1000);
        scan.setCaching(1050);

        rs = new ClientScanner(conf, scan, toB(TABLE_NAME));
        rs.next(1050);
        int count = 0;
        for (Result r : rs) {
            p(r);
            count++;
        }
        p("count=" + count);
        rs.close();
    }
}
