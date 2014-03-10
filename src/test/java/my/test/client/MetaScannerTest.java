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

import my.test.TestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.TableMetaScannerVisitor;
import org.apache.hadoop.hbase.client.Result;

public class MetaScannerTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new MetaScannerTest().run();
    }

    public void run() throws Exception {
        tableName = "MetaScannerTest";

        createTable("cf1", "cf2");

        //1. 这种方式无法看到结果
        MetaScanner.metaScan(sharedConf, new MyTableMetaScannerVisitor(sharedConf, toB(tableName)));

        //2. 这种方式可以
        p(MetaScanner.allTableRegions(sharedConf, null, toB(tableName), false));

        //3. 要是把MetaScanner.TableMetaScannerVisitor.processRow(Result)的"return false;"改成"return true;"则一样
    }

    static class MyTableMetaScannerVisitor extends TableMetaScannerVisitor {

        public MyTableMetaScannerVisitor(Configuration conf, byte[] tableName) {
            super(conf, tableName);
        }

        @Override
        public boolean processRowInternal(Result rowResult) throws IOException {
            System.out.println(rowResult);
            return true;
        }

    }
}
