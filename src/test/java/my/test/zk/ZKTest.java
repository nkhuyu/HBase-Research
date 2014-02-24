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
package my.test.zk;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class ZKTest implements org.apache.hadoop.hbase.Abortable {

    /**
     * @param args
     * @throws IOException 
     * @throws ZooKeeperConnectionException 
     * @throws KeeperException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws ZooKeeperConnectionException, IOException, KeeperException,
            InterruptedException {
        ZKTest t = new ZKTest();
        t.run();
    }

    Configuration conf = HBaseConfiguration.create();

    void run() throws ZooKeeperConnectionException, IOException, KeeperException, InterruptedException {

        zkclient();

        //zkserver();
    }

    void zkclient() throws ZooKeeperConnectionException, IOException, KeeperException {
        ZooKeeperWatcher zooKeeper = new ZooKeeperWatcher(conf, "My_ZKTest", this, true);
        String quorum = zooKeeper.getQuorum();

        System.out.println(quorum);
        for (String node : ZKUtil.listChildrenNoWatch(zooKeeper, "/hbase"))
            System.out.println(node);

        System.out.println(ZKUtil.getZooKeeperClusterKey(conf));

        ZKUtil.createNodeIfNotExistsAndWatch(zooKeeper, "/hbase/my", Bytes.toBytes("sss"));

        System.out.println(ZKUtil.getZooKeeperClusterKey(conf));

        ZKUtil.createNodeIfNotExistsAndWatch(zooKeeper, "/hbase/hbaseid", Bytes.toBytes("aaa"));

        ClusterId.setClusterId(zooKeeper, "setClusterId");

        ClusterId id = new ClusterId(zooKeeper, this);
        String s = id.getId();
        System.out.println(s);

        MyZooKeeperNodeTracker t = new MyZooKeeperNodeTracker(zooKeeper, "/hbase/mytracker", this);
        t.start();
        try {
            t.blockUntilAvailable();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void zkserver() throws IOException, InterruptedException {
        final MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster();
        File zkDataPath = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR));
        int zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 0);
        if (zkClientPort == 0) {
            throw new IOException("No config value for " + HConstants.ZOOKEEPER_CLIENT_PORT);
        }
        zooKeeperCluster.setDefaultClientPort(zkClientPort);
        int clientPort = zooKeeperCluster.startup(zkDataPath);

        System.out.println(clientPort);
    }

    @Override
    public void abort(String why, Throwable e) {
        new Error(why, e).printStackTrace();
    }

    @Override
    public boolean isAborted() {
        return false;
    }

    public static class MyZooKeeperNodeTracker extends ZooKeeperNodeTracker {
        public MyZooKeeperNodeTracker(ZooKeeperWatcher watcher, String node, Abortable abortable) {
            super(watcher, node, abortable);
        }
    }

}
