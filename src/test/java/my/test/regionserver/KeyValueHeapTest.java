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
package my.test.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;

import my.test.TestBase;

public class KeyValueHeapTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new KeyValueHeapTest().run();
    }

    public void run() throws Exception {
        List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
        MyKeyValueScanner kvs1 = new MyKeyValueScanner("kvs1", 1, 3, 5);
        MyKeyValueScanner kvs2 = new MyKeyValueScanner("kvs2", 2, 4, 6);
        MyKeyValueScanner kvs3 = new MyKeyValueScanner("kvs3");
        scanners.add(kvs1);
        scanners.add(kvs2);
        scanners.add(kvs3);

        KeyValueHeap kvh = new KeyValueHeap(scanners, KeyValue.COMPARATOR);
        p(kvh.next());
        p(kvh.seek(getKeyValue(4)));
        kvh.close();
    }

    static byte[] family = Bytes.toBytes("cf");
    static byte[] qualifier = Bytes.toBytes("q1");

    static KeyValue getKeyValue(int key) {
        KeyValue kv = new KeyValue(Bytes.toBytes(key), family, qualifier, 0, Bytes.toBytes(key * 10));
        kv.setMemstoreTS(key * 10);
        return kv;
    }

    static class MyKeyValueScanner implements KeyValueScanner {

        List<KeyValue> keyValues = new ArrayList<KeyValue>();

        int index;
        String name;

        public MyKeyValueScanner(String name, int... keys) {
            this.name = name;
            for (int key : keys) {
                keyValues.add(getKeyValue(key));
            }
        }

        @Override
        public String toString() {
            return "MyKeyValueScanner-" + name;
        }

        @Override
        public KeyValue peek() {
            if (index >= keyValues.size())
                return null;

            return keyValues.get(index);
        }

        @Override
        public KeyValue next() throws IOException {
            return keyValues.get(index++);
        }

        @Override
        public boolean seek(KeyValue key) throws IOException {
            if (index >= keyValues.size())
                return false;

            for (int i = index; i < keyValues.size(); i++)
                if (KeyValue.COMPARATOR.compare(key, keyValues.get(index++)) == 0)
                    return true;
            return false;
        }

        @Override
        public boolean reseek(KeyValue key) throws IOException {
            return seek(key);
        }

        @Override
        public long getSequenceID() {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
            return true;
        }

        @Override
        public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom) throws IOException {
            return forward ? reseek(kv) : seek(kv);
        }

        @Override
        public boolean realSeekDone() {
            return true;
        }

        @Override
        public void enforceSeek() throws IOException {
        }

        @Override
        public boolean isFileScanner() {
            return false;
        }

    }
}
