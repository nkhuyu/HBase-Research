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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import my.test.TestBase;

public class AppendTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new AppendTest().run();
    }

    public void run() throws Exception {
        tableName = "AppendTest";
        createTable("cf");
        try {
            HTableInterface t = getHTable();
            long ts = System.currentTimeMillis();
            Put put = new Put(toB("20000"));
            put.add(toB("cf"), toB("q1"), ts, toB("v2"));
            //put.add(toB("cf"), toB("q1"), toB("v2"));
            t.put(put);

            Delete d = new Delete(toB("20000"));
            t.delete(d);

            ts = System.currentTimeMillis() + 1;
            ts = System.currentTimeMillis();
            put = new Put(toB("20000"));
            put.add(toB("cf"), toB("q1"), ts, toB("v2"));
            //put.add(toB("cf"), toB("q1"), toB("v2"));
            t.put(put);

            //new T2().start();

            //new T().start();

            Get get = new Get(toB("20000"));
            p(t.get(get));

            t.getScanner(new Scan());
            t.getScanner(new Scan());

            //			Append append = new Append(toB("20007")); //可以指定一个不存在的rowKey
            //			//append.add(toB("cf1"), toB("q1"), toB("v3")); //但是不可以指定一个不存在的列族
            //			append.add(toB("cf"), toB("q2"), toB("v4"));
            //			t.append(append);
            //
            //			get = new Get(toB("20007"));
            //			p(t.get(get));
            //
            //			append = new Append(toB("20007"));
            //			append.setReturnResults(false); //不需要返回结果
            //			append.add(toB("cf"), toB("q4"), toB("v4"));
            //			Result r = t.append(append);
            //			p(r);
            //
            //			append = new Append(toB("20007"));
            //			append.setReturnResults(true); //需要返回结果，默认就是true
            //			append.add(toB("cf"), toB("q5"), toB("vf"));
            //			r = t.append(append); //r代表只返回新添加的字段
            //			p(r); //keyvalues={20007/cf:q5/1351045684751/Put/vlen=2/ts=0}
            //
            //			t.delete(new Delete(toB("20007")));
        } finally {
            // deleteTable(tableName);
        }

    }

    class T extends Thread {

        public void run() {
            HTableInterface t = getHTable();
            Get get = new Get(Bytes.toBytes(("20000")));
            try {
                System.out.println(t.get(get));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    class T2 extends Thread {

        public void run() {
            HTableInterface t = getHTable();

            Put put = new Put(Bytes.toBytes("20000"));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("v2"));
            try {
                t.put(put);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
