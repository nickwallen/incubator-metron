/**
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
package org.apache.metron.enrichment.lookup.accesstracker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.metron.enrichment.lookup.LookupKey;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.hbase.client.HBaseClient;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class PersistentAccessTracker implements AccessTracker {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 1L;

    public static class AccessTrackerKey {
        String name;
        String containerName;
        long timestamp;
        public AccessTrackerKey(String name, String containerName, long timestamp) {
            this.name = name;
            this.containerName = containerName;
            this.timestamp = timestamp;
        }

        public byte[] toRowKey() {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(os);
            try {
                dos.writeUTF(name);
                dos.writeLong(timestamp);
                dos.writeUTF(containerName);
                dos.flush();
            } catch (IOException e) {
                throw new RuntimeException("Unable to write rowkey: " + this, e);
            }

            return os.toByteArray();
        }

        public static byte[] getTimestampScanKey(String name, long timestamp) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(os);
            try {
                dos.writeUTF(name);
                dos.writeLong(timestamp);
            } catch (IOException e) {
                throw new RuntimeException("Unable to create scan key " , e);
            }

            return os.toByteArray();
        }

        public static AccessTrackerKey fromRowKey(byte[] rowKey) {
            ByteArrayInputStream is = new ByteArrayInputStream(rowKey);
            DataInputStream dis = new DataInputStream(is);
            try {
                String name = dis.readUTF();
                long timestamp = dis.readLong();
                String containerName = dis.readUTF();
                return new AccessTrackerKey(name, containerName, timestamp);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read rowkey: ", e);
            }
        }
    }

    private static class Persister extends TimerTask {
        PersistentAccessTracker tracker;
        public Persister(PersistentAccessTracker tracker) {
            this.tracker = tracker;
        }
        /**
         * The action to be performed by this timer task.
         */
        @Override
        public void run() {
            tracker.persist(false);
        }
    }

    private final Object sync = new Object();
    public String accessTrackerColumn = "v";
    private String accessTrackerColumnFamily;
    private AccessTracker underlyingTracker;
    private long timestamp = System.currentTimeMillis();
    private String tableName;
    private String containerName;
    private Timer timer;
    private long maxMillisecondsBetweenPersists;
    private HBaseClient hbaseClient;

    public PersistentAccessTracker( String tableName,
                                    String containerName,
                                    String columnFamily,
                                    AccessTracker underlyingTracker,
                                    long maxMillisecondsBetweenPersists,
                                    HBaseConnectionFactory connectionFactory,
                                    Configuration configuration) {
        this.containerName = containerName;
        this.tableName = tableName;
        this.accessTrackerColumnFamily = columnFamily;
        this.underlyingTracker = underlyingTracker;
        this.maxMillisecondsBetweenPersists = maxMillisecondsBetweenPersists;
        this.timer = new Timer();
        if(maxMillisecondsBetweenPersists > 0) {
            this.timer.scheduleAtFixedRate(new Persister(this), maxMillisecondsBetweenPersists, maxMillisecondsBetweenPersists);
        }
        this.hbaseClient = HBaseClient.createSyncClient(connectionFactory, configuration, tableName);
    }

    public void persist(boolean force) {
        synchronized(sync) {
            if(force || (System.currentTimeMillis() - timestamp) >= maxMillisecondsBetweenPersists) {
                //persist
                try {
                    AccessTrackerKey key = new AccessTrackerKey(tableName, containerName, timestamp);
                    persistTracker(key, underlyingTracker);
                    timestamp = System.currentTimeMillis();
                    reset();
                } catch (IOException e) {
                    LOG.error("Unable to persist access tracker.", e);
                }
            }
        }
    }

    private void persistTracker(PersistentAccessTracker.AccessTrackerKey key,
                                AccessTracker underlyingTracker) throws IOException {

        byte[] value = serializeTracker(underlyingTracker);
        ColumnList columns = new ColumnList().addColumn(accessTrackerColumnFamily, accessTrackerColumn, value);
        hbaseClient.addMutation(key.toRowKey(), columns, Durability.USE_DEFAULT);
        hbaseClient.mutate();
    }

    private byte[] serializeTracker(AccessTracker tracker) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(tracker);
        oos.flush();
        oos.close();
        return bos.toByteArray();
    }

    @Override
    public void logAccess(LookupKey key) {
        synchronized (sync) {
            underlyingTracker.logAccess(key);
            if (isFull()) {
                persist(true);
            }
        }
    }

    @Override
    public void configure(Map<String, Object> config) {
        underlyingTracker.configure(config);
    }

    @Override
    public boolean hasSeen(LookupKey key) {
        synchronized(sync) {
            return underlyingTracker.hasSeen(key);
        }
    }

    @Override
    public String getTableName() {
        return underlyingTracker.getTableName();
    }

    @Override
    public AccessTracker union(AccessTracker tracker) {
        PersistentAccessTracker t1 = (PersistentAccessTracker)tracker;
        underlyingTracker = underlyingTracker.union(t1.underlyingTracker);
        return this;
    }

    @Override
    public void reset() {
        synchronized(sync) {
            underlyingTracker.reset();
        }
    }

    @Override
    public boolean isFull() {
        synchronized (sync) {
            return underlyingTracker.isFull();
        }
    }

    @Override
    public void cleanup() throws IOException {
        synchronized(sync) {
            try {
                persist(true);
            }
            catch(Throwable t) {
                LOG.error("Unable to persist underlying tracker", t);
            }
            underlyingTracker.cleanup();
            hbaseClient.close();
        }
    }
}
