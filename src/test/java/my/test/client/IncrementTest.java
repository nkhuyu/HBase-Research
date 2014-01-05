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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import my.test.TestBase;

public class IncrementTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new IncrementTest().run();
    }

    public void run() throws Exception {
        tableName = "IncrementTest";
        byte[] cf = toB("cf");
        createTable("cf");
        try {
            HTableInterface t = getHTable();

            Put put = new Put(toB("20000"));
            put.add(cf, toB("q1"), toB(10L));
            t.put(put);

            Get get = new Get(toB("20000"));
            p(t.get(get));

            Increment increment = new Increment(toB("20000"));
            increment.addColumn(cf, toB("q1"), 20L);
            t.increment(increment);

            get = new Get(toB("20000"));
            p(Bytes.toLong(t.get(get).getValue(cf, toB("q1"))));

            t.incrementColumnValue(toB("20000"), cf, toB("q1"), 30L);

            get = new Get(toB("20000"));
            p(Bytes.toLong(t.get(get).getValue(cf, toB("q1"))));
        } finally {
            //deleteTable(tableName);
        }

    }

}
