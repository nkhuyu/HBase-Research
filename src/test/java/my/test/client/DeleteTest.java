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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import my.test.TestBase;
public class DeleteTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new DeleteTest().run();
    }

    void printIndexTable(HTableInterface t) throws Exception {

        System.out.println();

        ResultScanner resultScanner = t.getScanner(new Scan());
        Result result = resultScanner.next();
        while (result != null) {
            System.out.println("Result " + Bytes.toStringBinary(result.getRow()) + " " + result);
            result = resultScanner.next();
        }
        resultScanner.close();
    }

    @SuppressWarnings("deprecation")
    public void run() throws Exception {
        tableName = "DeleteTest";
        byte[] cf = toB("cf");
        byte[] cf2 = toB("cf2");
        //createTable(tableName, "cf", "cf2");
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        HTableDescriptor htd = new HTableDescriptor(tableName);

        String[] familyNames = { "cf", "cf2" };

        //String[] familyNames = { "cf" };

        for (String familyName : familyNames) {
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            hcd.setMaxVersions(3);
            hcd.setCompressionType(Compression.Algorithm.GZ);
            hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            htd.addFamily(hcd);
        }
        //deleteTable(tableName);
        if (!admin.tableExists(htd.getName())) {
            admin.createTable(htd);
        }
        try {
            HTableInterface t = getHTable();
            Delete d;
            Put put;
            Get get;

            put = new Put(toB("20000"));
            put.add(cf, toB("q1"), 4, null);
            t.put(put);

            printIndexTable(t);

            d = new Delete(toB("20000"));
            d.deleteColumn(cf, toB("q1"), 4);
            t.delete(d);

            printIndexTable(t);

            d = new Delete(toB("20000"), 3, null);
            t.delete(d);
            //Thread.sleep(1000);

            put = new Put(toB("20000"));
            put.add(cf, toB("q1"), 4, toB("v2"));
            t.put(put);

            get = new Get(toB("20000"));
            get.addColumn(cf, toB("q1"));
            p(t.get(get));

            put = new Put(toB("20003"));
            put.add(cf, toB("q1"), 6, toB("v2"));
            t.put(put);
            d = new Delete(toB("20003"), 6, null);
            t.delete(d);
            //Thread.sleep(1000);

            get = new Get(toB("20003"));
            get.addColumn(cf, toB("q1"));
            p(t.get(get));


            //Delete不会删除列族，只会删除某行记录下面的字段，只是针对单行记录的操作
            //下面的代码就是删除rowKey=20007下面的所有字段
            d = new Delete(toB("20007"));
            //d.deleteFamily(cf);
            t.delete(d);

            ResultScanner rs = t.getScanner(new Scan());
            Result[] results = rs.next(100);
            for (Result r : results)
                p(r);

            get = new Get(toB("20007"));
            p(t.get(get)); //keyvalues=NONE 

            put = new Put(toB("20007"));
            put.add(cf, toB("q1"), toB("v1"));
            put.add(cf2, toB("q2"), toB("v2"));
            t.put(put);
            d = new Delete(toB("20007"));
            d.deleteFamily(cf);
            t.delete(d);

            get = new Get(toB("20007"));
            p(t.get(get));

            d = new Delete(toB("20007"));
            t.delete(d);

            get = new Get(toB("20007"));
            p(t.get(get));

            //当重复执行这段代码时，虽然时间戳为1，2的版本重复put，但是因为最新版本已到5了，所以是无效的，
            //只有第一次执行这段代码时打印的结果才是5个，第二开始以后都是三个。
            put = new Put(toB("20008"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf, toB("q1"), 4, toB("v1"));
            put.add(cf, toB("q1"), 5, toB("v1"));
            t.put(put);
            get = new Get(toB("20008"));
            get.setMaxVersions(5);
            // 20008/cf:q1/5/Put/vlen=2/ts=0的格式是: rowKey/列族名:列名/时间戳/KV类型/列值长度/memstore时间戳
            //keyvalues={20008/cf:q1/5/Put/vlen=2/ts=0, 20008/cf:q1/4/Put/vlen=2/ts=0, 20008/cf:q1/3/Put/vlen=2/ts=0, 20008/cf:q1/2/Put/vlen=2/ts=0, 20008/cf:q1/1/Put/vlen=2/ts=0}
            p(t.get(get));

            d = new Delete(toB("20008"), 2, null);
            t.delete(d);
            //keyvalues={20008/cf:q1/5/Put/vlen=2/ts=0, 20008/cf:q1/4/Put/vlen=2/ts=0, 20008/cf:q1/3/Put/vlen=2/ts=0}
            p(t.get(get));

            //测试public Delete(byte[] row)
            put = new Put(toB("20009"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf2, toB("q2"), 1, toB("v2"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf2, toB("q2"), 2, toB("v2"));
            t.put(put);
            get = new Get(toB("20009"));
            get.setMaxVersions(5);
            p(t.get(get));
            //删除rowkey=20009这行中的所有列族的所有版本
            d = new Delete(toB("20009"));
            t.delete(d);
            p(t.get(get)); //keyvalues=NONE

            //测试public Delete(byte[] row, long timestamp, RowLock rowLock)
            put = new Put(toB("20010"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf2, toB("q2"), 1, toB("v2"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf2, toB("q2"), 2, toB("v2"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf2, toB("q2"), 3, toB("v2"));
            t.put(put);
            get = new Get(toB("20010"));
            get.setMaxVersions(5);
            p(t.get(get));
            //删除rowkey=20010这行中所有列族的timestamp<=2的版本，只剩3的
            d = new Delete(toB("20010"), 2, null);
            t.delete(d);
            p(t.get(get)); //keyvalues={20010/cf:q1/3/Put/vlen=2/ts=0, 20010/cf2:q2/3/Put/vlen=2/ts=0}

            put = new Put(toB("20011"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf2, toB("q2"), 1, toB("v2"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf2, toB("q2"), 2, toB("v2"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf2, toB("q2"), 3, toB("v2"));
            t.put(put);
            get = new Get(toB("20011"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20011"));
            //删除cf2的所有版本
            d.deleteFamily(cf2);
            t.delete(d);
            p(t.get(get)); //keyvalues={20011/cf:q1/3/Put/vlen=2/ts=0, 20011/cf:q1/2/Put/vlen=2/ts=0, 20011/cf:q1/1/Put/vlen=2/ts=0}

            put = new Put(toB("20012"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf2, toB("q2"), 1, toB("v2"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf2, toB("q2"), 2, toB("v2"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf2, toB("q2"), 3, toB("v2"));
            t.put(put);
            get = new Get(toB("20012"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20013"));
            //删除cf2这个列族版本<=timestamp的数据，cf不受影响
            d.deleteFamily(cf2, 2);
            t.delete(d);
            p(t.get(get)); //keyvalues={20012/cf:q1/3/Put/vlen=2/ts=0, 20012/cf:q1/2/Put/vlen=2/ts=0, 20012/cf:q1/1/Put/vlen=2/ts=0, 20012/cf2:q2/3/Put/vlen=2/ts=0}

            put = new Put(toB("20013"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf, toB("q2"), 1, toB("v1"));
            put.add(cf, toB("q2"), 2, toB("v1"));
            t.put(put);
            get = new Get(toB("20013"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20013"));
            //删除cf.q1这个列所有版本的数据
            d.deleteColumns(cf, toB("q1"));
            t.delete(d);
            p(t.get(get)); //keyvalues={20013/cf:q2/2/Put/vlen=2/ts=0, 20013/cf:q2/1/Put/vlen=2/ts=0}

            put = new Put(toB("20014"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf, toB("q2"), 1, toB("v1"));
            put.add(cf, toB("q2"), 2, toB("v1"));
            t.put(put);
            get = new Get(toB("20014"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20014"));
            //删除cf.q1这个列版本<=2的数据
            d.deleteColumns(cf, toB("q1"), 2);
            t.delete(d);
            p(t.get(get)); //keyvalues={20014/cf:q1/3/Put/vlen=2/ts=0, 20014/cf:q2/2/Put/vlen=2/ts=0, 20014/cf:q2/1/Put/vlen=2/ts=0}

            put = new Put(toB("20015"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            t.put(put);
            get = new Get(toB("20015"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20015"));
            //删除cf.q1这个列最新版本的数据(即时间戳为3)
            d.deleteColumn(cf, toB("q1"));
            t.delete(d);
            p(t.get(get)); //keyvalues={20015/cf:q1/2/Put/vlen=2/ts=0, 20015/cf:q1/1/Put/vlen=2/ts=0}

            put = new Put(toB("20016"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            t.put(put);
            get = new Get(toB("20016"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20016"));
            //删除cf.q1这个列时间戳与2相等的数据
            d.deleteColumn(cf, toB("q1"), 2);
            t.delete(d);
            p(t.get(get)); //keyvalues={20016/cf:q1/3/Put/vlen=2/ts=0, 20016/cf:q1/1/Put/vlen=2/ts=0}

            put = new Put(toB("20017"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf2, toB("q2"), 1, toB("v2"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf2, toB("q2"), 2, toB("v2"));
            put.add(cf, toB("q1"), 3, toB("v1"));
            put.add(cf2, toB("q2"), 3, toB("v2"));
            t.put(put);
            get = new Get(toB("20017"));
            get.setMaxVersions(5);
            p(t.get(get));

            //如果在Delete构造函数指定了时间戳，但是deleteFamily没有指定，这个时间戳是无效的，deleteFamily保持自己的语义
            //其他deleteColumns、deleteColumn都类似，Delete构造函数指定的时间戳只有在不调用其他deleteXXX方法时才有效。
            d = new Delete(toB("20017"), 2, null);
            //删除cf2的所有版本
            d.deleteFamily(cf2);
            t.delete(d);
            p(t.get(get)); //keyvalues={20017/cf:q1/3/Put/vlen=2/ts=0, 20017/cf:q1/2/Put/vlen=2/ts=0, 20017/cf:q1/1/Put/vlen=2/ts=0}

            //测试org.apache.hadoop.hbase.client.Delete.addDeleteMarker(KeyValue)
            put = new Put(toB("20018"));
            put.add(cf, toB("q1"), 1, toB("v1"));
            put.add(cf, toB("q1"), 2, toB("v1"));
            put.add(cf, toB("q2"), 1, toB("v2"));
            t.put(put);
            get = new Get(toB("20018"));
            get.setMaxVersions(5);
            p(t.get(get));

            d = new Delete(toB("20018"));
            //The recently added KeyValue is not of type delete. Rowkey: 20018
            KeyValue kv = new KeyValue(toB("20018"), cf, toB("q1"), 2, KeyValue.Type.Put);
            //The row in the recently added KeyValue 20019 doesn't match the original one 20018
            kv = new KeyValue(toB("20019"), cf, toB("q1"), 2, KeyValue.Type.Delete);
            kv = new KeyValue(toB("20018"), cf, toB("q1"), 2, KeyValue.Type.Delete);
            d.addDeleteMarker(kv);
            t.delete(d);
            p(t.get(get)); //keyvalues={20018/cf:q1/1/Put/vlen=2/ts=0, 20018/cf:q2/1/Put/vlen=2/ts=0}

        } finally {
            //deleteTable(tableName);
        }

    }
}
