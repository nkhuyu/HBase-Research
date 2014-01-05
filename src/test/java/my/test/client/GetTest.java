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

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import my.test.TestBase;

public class GetTest extends TestBase {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        new GetTest().run();
    }

    public void run() throws Exception {
        tableName = "GetTest";
        createTable("cf1", "cf2");
        byte[] row = toB("111");
        byte[] family1 = toB("cf1");
        byte[] family2 = toB("cf2");

        byte[] qualifier1 = toB("my qualifier1");
        byte[] qualifier2 = toB("my qualifier2");

        Get get = new Get(row);
        get.addFamily(family1);
        get.addFamily(family2);

        get.addColumn(family2, qualifier1);
        get.addColumn(family2, qualifier2);

        p(get.getFingerprint());

        p(get.toMap(4));
        p(get.toJSON());

        p(get);

        get = new Get(toB("200"));
        HTableInterface t = getHTable();
        p(t.get(get));

        Put put = new Put(toB("20000"));
        put.add(toB("cf1"), toB("q1"), toB("v2"));
        //t.put(put);

        get = new Get(toB("20000"));
        p(t.get(get));

        Append append = new Append(toB("20000"));
        append.add(toB("cf1"), toB("q1"), toB("v3"));
        append.add(toB("cf1"), toB("q2"), toB("v4"));
        t.append(append);

        get = new Get(toB("20000"));
        p(t.get(get));
    }

}
