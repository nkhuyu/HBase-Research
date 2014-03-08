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
import java.util.Map;

import my.test.TestBase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class HRegionTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new HRegionTest().run();
    }

    Configuration conf = HBaseConfiguration.create();
    String tableName = "mytest";
    HRegionInfo ri = new HRegionInfo(toB(tableName), toB("10000"), toB("99999"), true, 1000);

    void run() throws Exception {
        //testHRegionInfo();
        testHRegion();
    }

    void testHRegion() throws Exception {

        //deleteRootDir();

        //createHRegion();
        //openHRegion();
        //flushcache();
        //testHLog();

        //        testPut(10000, 30000, 50000);
        //        testPut(20000, 40000, 60000);

        //test_doMiniBatchPut();

        //testGet();

        testScan();

        //conf.set("hbase.hstore.compaction.min.size", "110");
        //compactStores();
    }

    void testHRegionInfo() throws Exception {
        System.out.println(HRegionInfo.ROOT_REGIONINFO);
        System.out.println(HRegionInfo.ROOT_REGIONINFO.getEncodedName());
        System.out.println(HRegionInfo.FIRST_META_REGIONINFO);
        System.out.println(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());

        HRegionInfo ri = new HRegionInfo(toB("tableName"), toB("startKey"), toB("endKey"), true, 1000);

        System.out.println();
        System.out.println(ri.getEncodedName());

        String regionName = "tableName,startKey,1000.acb7ac0a5959190a3d597b30b7dbba4b.";

        System.out.println(toS(HRegionInfo.getTableName(toB(regionName))));

        System.out.println();

        for (byte[] bytes : HRegionInfo.parseRegionName(toB(regionName)))
            System.out.println(toS(bytes));

        System.out.println(HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString());

        //System.out.println(ri.getTableDesc());
    }

    void deleteRootDir() throws Exception {
        System.out.println("deleteRootDir: " + getRootDir());
        FileSystem fs = FileSystem.get(conf);
        fs.delete(getRootDir(), true);
    }

    Path getRootDir() throws Exception {
        Path rootdir = FSUtils.getRootDir(conf);
        return rootdir;
    }

    void compactStores(HRegionInfo ri) throws Exception {
        HRegion region = getHRegion();
        //region.compactStores();
        region.compactStores(true);
    }

    HTableDescriptor getHTableDescriptor() {
        //String[] familyNames = { "cf1", "cf2" };

        String[] familyNames = { "cf1" };

        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.setMemStoreFlushSize(512 * 1024 * 1024);

        for (String familyName : familyNames) {
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            hcd.setCompressionType(Compression.Algorithm.GZ);
            //hcd.setBloomFilterType(StoreFile.BloomType.ROW);
            hcd.setBloomFilterType(StoreFile.BloomType.ROWCOL);
            //hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            htd.addFamily(hcd);
        }

        return htd;
    }

    FileSystem getFileSystem() throws Exception {
        return FileSystem.get(conf);
    }

    HRegionInfo getHRegionInfo() {
        return new HRegionInfo(toB(tableName), toB("10000"), toB("99999"), true, 1000);
    }

    HLog getHLog() throws Exception {
        HRegionInfo ri = getHRegionInfo();
        Path tableDir = HTableDescriptor.getTableDir(getRootDir(), ri.getTableName());
        Path regionDir = HRegion.getRegionDir(tableDir, ri.getEncodedName());

        List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
        listeners.add(new MyWALActionsListener());
        HLog hlog = new HLog(getFileSystem(), new Path(regionDir, HConstants.HREGION_LOGDIR_NAME), new Path(regionDir,
                HConstants.HREGION_OLDLOGDIR_NAME), conf, listeners, false, "myhlog", false);

        return hlog;
    }

    void createHRegion() throws Exception {
        HRegion region = null;
        //region = HRegion.createHRegion(ri, getRootDir(), conf, getHTableDescriptor());
        region = HRegion.createHRegion(ri, getRootDir(), conf, getHTableDescriptor(), getHLog());
        System.out.println(region);
    }

    int ttl = 10; //10妙

    void test_doMiniBatchPut() throws Exception {
        HRegion region = null;
        HTableDescriptor htd = getHTableDescriptor();
        //for (HColumnDescriptor hcd : htd.getColumnFamilies())
        //hcd.setTimeToLive(ttl);
        //htd.setReadOnly(true);

        conf.setBoolean("hbase.store.delete.expired.storefile", true);
        //conf.setInt("hbase.hstore.compaction.max.size", 10);
        conf.setInt("hbase.hstore.compaction.max", 2);

        region = HRegion.openHRegion(ri, htd, getHLog(), conf, new MyRegionServerServices(), new MyCancelableProgressable());
        System.out.println(region);

        Put[] puts = new Put[3];
        int count = 0;
        for (int i = 10000; i < 10003; i++) {
            Put put = new Put(toB("" + i));
            //put.setWriteToWAL(false);
            put.add(toB("cf1"), toB("c"), toB("myvalue"));
            puts[count++] = put;

        }
        //region.put(puts);
        //region.flushcache();
        //region.compactStores(true);
        region.compactStores();

        MyCompactThread t = new MyCompactThread();
        t.region = region;
        //t.start();

        t = new MyCompactThread();
        t.region = region;
        //t.start();

        region.close();
    }

    void testGet() throws Exception {
        HRegion region = getHRegion();
        Get get = new Get(toB("10001"));

        Result result = region.get(get);

        p(result);
    }

    HRegion getHRegion() throws Exception {
        HRegion region = null;
        HTableDescriptor htd = getHTableDescriptor();
        //htd.setReadOnly(true);
        region = HRegion.openHRegion(ri, htd, null, conf, null, new MyCancelableProgressable());
        return region;
    }

    void testScan() throws Exception {
        testPut(false, 70000, 80000);
        HRegion region = getHRegion();

        Delete d = new Delete(toB("20000"));
        d.deleteColumn(toB("cf1"), toB("q2"));
        //region.delete(d, false);

        for (int i = 0; i < 1; i++) {
            Scan scan = new Scan();
            scan = new Scan(toB("30000"), toB("80001"));
                        scan.addColumn(toB("cf1"), toB("q1"));
                        scan.addColumn(toB("cf1"), toB("q2"));
            //scan.addColumn(toB("cf1"), toB("q3"));
            //scan.addColumn(toB("cf2"), toB("q2"));

            Filter f = null;
            f = new RowFilter(CompareOp.EQUAL, new BinaryComparator(toB("30000")));
            //f = new SingleColumnValueFilter(toB("cf2"), toB("q2"), CompareOp.EQUAL, toB("myvalue1-30000"));
            //((SingleColumnValueFilter)f).setFilterIfMissing(true);
            scan.setFilter(f);

            scan.setBatch(2);
            scan.setLoadColumnFamiliesOnDemand(true);

            //scan.setTimeRange(20003, 30001);

            scan.setTimeRange(10000, 99999);

            scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);

            RegionScanner regionScanner = region.getScanner(scan);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (regionScanner.next(results) || !results.isEmpty()) {
                p(results.size());
                p(results);
                p();
                results.clear();
            }
            regionScanner.close();
        }
    }

    HRegion openHRegion() throws Exception {
        HRegion region = null;
        HTableDescriptor htd = getHTableDescriptor();
        //htd.setReadOnly(true);
        region = HRegion.openHRegion(ri, htd, getHLog(), conf, null, new MyCancelableProgressable());

        return region;
    }

    void testPut(boolean flush, int... keys) throws Exception {

        HRegion region = openHRegion();
        for (int key : keys) {
            int i2 = key;
            Put put = new Put(toB("" + i2));
            put.setDurability(Durability.SYNC_WAL);
            put.add(toB("cf1"), toB("q1"), i2, toB("myvalue1-" + i2));
            put.add(toB("cf1"), toB("q2"), i2, toB("myvalue2-" + i2));
            put.add(toB("cf1"), toB("q3"), i2, toB("myvalue2-" + i2));

            put.add(toB("cf1"), toB("q1"), i2 + 1, toB("myvalue1-" + i2));
            put.add(toB("cf1"), toB("q2"), i2 + 1, toB("myvalue2-" + i2));
            put.add(toB("cf1"), toB("q3"), i2 + 1, toB("myvalue2-" + i2));

            //                put.add(toB("cf2"), toB("q1"), i2, toB("myvalue1-" + i2));
            //                put.add(toB("cf2"), toB("q2"), i2, toB("myvalue2-" + i2));
            //                put.add(toB("cf2"), toB("q3"), i2, toB("myvalue2-" + i2));

            region.put(put);
        }
        if (flush) {
            region.flushcache();
            region.close();
        }
    }

    void testPut(int... keys) throws Exception {
        testPut(true, keys);
    }

    @SuppressWarnings("deprecation")
    void testPut() throws Exception {
        HRegion region = openHRegion();

        int count = 5;
        for (int j = 0; j < count; j++) {
            for (int i = 10000; i < 10003; i++) {
                int i2 = j * 10000 + i;
                Put put = new Put(toB("" + i2));
                put.setWriteToWAL(true);
                put.add(toB("cf1"), toB("q1"), i2, toB("myvalue1-" + i2));
                put.add(toB("cf1"), toB("q2"), i2, toB("myvalue2-" + i2));
                put.add(toB("cf1"), toB("q3"), i2, toB("myvalue2-" + i2));

                put.add(toB("cf1"), toB("q1"), i2 + 1, toB("myvalue1-" + i2));
                put.add(toB("cf1"), toB("q2"), i2 + 1, toB("myvalue2-" + i2));
                put.add(toB("cf1"), toB("q3"), i2 + 1, toB("myvalue2-" + i2));

                //                put.add(toB("cf2"), toB("q1"), i2, toB("myvalue1-" + i2));
                //                put.add(toB("cf2"), toB("q2"), i2, toB("myvalue2-" + i2));
                //                put.add(toB("cf2"), toB("q3"), i2, toB("myvalue2-" + i2));
                Integer lockid = region.obtainRowLock(put.getRow());
                try {
                    region.put(put, lockid);

                    MyThread t = new MyThread();
                    t.region = region;
                    t.put = put;
                    t.lockid = lockid;
                    //t.start();
                } finally {
                    if (lockid != null)
                        region.releaseRowLock(lockid);
                }

            }
            region.flushcache();
        }
        region.close();
    }

    @SuppressWarnings("deprecation")
    void flushcache() throws Exception {
        HRegion region = null;
        HTableDescriptor htd = getHTableDescriptor();
        //htd.setReadOnly(true);
        region = HRegion.openHRegion(ri, htd, getHLog(), conf, null, new MyCancelableProgressable());
        System.out.println(region);

        for (int i = 10000; i < 10003; i++) {
            Put put = new Put(toB("" + i));
            put.setWriteToWAL(false);
            put.add(toB("cf1"), toB("c"), toB("myvalue"));
            Integer lockid = region.obtainRowLock(put.getRow());
            try {
                region.put(put, lockid);
            } finally {
                if (lockid != null)
                    region.releaseRowLock(lockid);
            }

        }
        region.flushcache();

        region.close();
    }

    static class MyCompactThread extends Thread {
        HRegion region;

        public void run() {
            try {
                region.compactStores();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class MyThread extends Thread {
        HRegion region;
        Put put;
        Integer lockid;

        @SuppressWarnings("deprecation")
        public void run() {
            try {
                region.put(put, lockid);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (lockid != null)
                    region.releaseRowLock(lockid);
            }
        }
    }

    void testHLog() throws Exception {
        System.out.println("testHLog");
        HLog hlog = getHLog();
        WALEdit edits = new WALEdit();

        long now = System.currentTimeMillis();
        for (int i = 10000; i < 12000; i++) {
            KeyValue kv = new KeyValue(toB("" + i), toB("cf1"), toB("c"), now, Type.Put, toB("myvalue" + i));
            edits.add(kv);
        }

        HRegionInfo ri = getHRegionInfo();
        byte[] tableName = ri.getTableName();
        HTableDescriptor htd = getHTableDescriptor();

        hlog.append(ri, tableName, edits, now, htd);
        hlog.append(ri, tableName, edits, now, htd);
        long logSeqId = hlog.startCacheFlush(ri.getEncodedNameAsBytes());
        hlog.completeCacheFlush(ri.getEncodedNameAsBytes(), tableName, logSeqId, false);
        hlog.rollWriter(true);
        hlog.close();
        //hlog.closeAndDelete();
    }

    //    static byte[] toB(String str) {
    //        return Bytes.toBytes(str);
    //    }
    //
    //    static String toS(byte[] bytes) {
    //        return Bytes.toString(bytes);
    //    }

    static class MyCancelableProgressable implements CancelableProgressable {

        @Override
        public boolean progress() {
            return false;
        }

    }

    static class MyWALActionsListener implements WALActionsListener {
        static final Log log = LogFactory.getLog(HLog.class);

        @Override
        public void preLogRoll(Path oldPath, Path newPath) throws IOException {
            log.error("preLogRoll: oldPath=" + oldPath + ", newPath=" + newPath);
        }

        @Override
        public void postLogRoll(Path oldPath, Path newPath) throws IOException {
            log.error("preLogRoll: oldPath=" + oldPath + ", newPath=" + newPath);
        }

        @Override
        public void preLogArchive(Path oldPath, Path newPath) throws IOException {
            log.error("preLogArchive: oldPath=" + oldPath + ", newPath=" + newPath);
        }

        @Override
        public void postLogArchive(Path oldPath, Path newPath) throws IOException {
            log.error("postLogArchive: oldPath=" + oldPath + ", newPath=" + newPath);
        }

        @Override
        public void logRollRequested() {
            log.error("logRollRequested");
        }

        @Override
        public void logCloseRequested() {
            log.error("logCloseRequested");
        }

        @Override
        public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit) {
            log.error("visitLogEntryBeforeWrite");
        }

        @Override
        public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey, WALEdit logEdit) {
            log.error("visitLogEntryBeforeWrite");
        }

    }

    static class MyRegionServerServices implements RegionServerServices {
        RegionServerAccounting regionServerAccounting = new RegionServerAccounting();

        @Override
        public void addToOnlineRegions(HRegion r) {

        }

        @Override
        public boolean removeFromOnlineRegions(String encodedRegionName) {
            return false;
        }

        @Override
        public HRegion getFromOnlineRegions(String encodedRegionName) {
            return null;
        }

        @Override
        public List<HRegion> getOnlineRegions(byte[] tableName) throws IOException {
            return null;
        }

        @Override
        public Configuration getConfiguration() {
            return null;
        }

        @Override
        public ZooKeeperWatcher getZooKeeper() {
            return null;
        }

        @Override
        public CatalogTracker getCatalogTracker() {
            return null;
        }

        @Override
        public ServerName getServerName() {
            return null;
        }

        @Override
        public void abort(String why, Throwable e) {

        }

        @Override
        public boolean isAborted() {
            return false;
        }

        @Override
        public void stop(String why) {

        }

        @Override
        public boolean isStopped() {
            return false;
        }

        @Override
        public boolean isStopping() {
            return false;
        }

        @Override
        public HLog getWAL() {
            return null;
        }

        @Override
        public CompactionRequestor getCompactionRequester() {
            return null;
        }

        @Override
        public FlushRequester getFlushRequester() {
            return null;
        }

        @Override
        public RegionServerAccounting getRegionServerAccounting() {
            return regionServerAccounting;
        }

        @Override
        public void postOpenDeployTasks(HRegion r, CatalogTracker ct, boolean daughter) throws KeeperException, IOException {

        }

        @Override
        public RpcServer getRpcServer() {
            return null;
        }

        //@Override
        public Map<byte[], Boolean> getRegionsInTransitionInRS() {
            return null;
        }

        @Override
        public FileSystem getFileSystem() {
            return null;
        }

        //@Override
        public Leases getLeases() {
            return null;
        }

        //@Override
        public boolean removeFromRegionsInTransition(HRegionInfo hri) {
            return false;
        }

        //@Override
        public boolean containsKeyInRegionsInTransition(HRegionInfo hri) {
            return false;
        }

        @Override
        public HLog getWAL(HRegionInfo regionInfo) throws IOException {
            return null;
        }

    }
}
