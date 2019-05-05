package org.vidi.distribution.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/**
 * Distribution lock using Zookeeper ephemeral sequential ZNode and event.
 *
 * @author vidi
 */
public class ZookeeperDistributedLock implements DistributedLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperDistributedLock.class);

    /**
     * Zookeeper client.
     */
    private ZooKeeper zk;

    /**
     * The zookeeper path of the lock.
     */
    private String root = "/locks";

    /**
     * Name of this lock.
     */
    private String lockName;

    /**
     * The sequential ZNode which current thread created.
     */
    private ThreadLocal<String> nodeId = new ThreadLocal<>();

    /**
     * Waiting Zookeeper client connecting to Zookeeper server.
     */
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private final static int SESSION_TIMEOUT = 3000;
    private final static byte[] DATA = new byte[0];

    /**
     * Construct Zookeeper client, and register Zookeeper Watcher. Then suspend the thread.
     * Zookeeper will send SyncConnected event while Zookeeper client has connected to Zookeeper server.
     * Watcher call {@link java.util.concurrent.CountDownLatch#countDown()} to active current thread
     * and create the root ZNode while Watcher find current connection is created.
     *
     * @param config   Zookeeper connection information, eg: hostname:port.
     * @param lockName lock name.
     */
    public ZookeeperDistributedLock(String config, String lockName) {
        this.lockName = lockName;

        try {
            zk = new ZooKeeper(config, SESSION_TIMEOUT, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            });

            connectedSignal.await();
            Stat stat = zk.exists(root, false);
            if (null == stat) {
                zk.create(root, DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class LockWatcher implements Watcher {
        private CountDownLatch latch;

        LockWatcher(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                latch.countDown();
            }
        }
    }

    /**
     * The function to get the lock.
     * 1. Create the ephemeral sequential ZNode.
     * 2. Get all the children ZNode and sort these ZNode.
     * 3. Check if the value of current thread's ZNode is the least. If true, get the lock successful; if false, wait for get the lock.
     */
    public void lock() {
        try {
            String currentNode = zk.create(root + "/" + lockName, DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);

            List<String> subNodes = zk.getChildren(root, false);
            TreeSet<String> sortedNodes = new TreeSet<>();
            for (String node : subNodes) {
                sortedNodes.add(root + "/" + node);
            }

            String smallNode = sortedNodes.first();
            String preNode = sortedNodes.lower(currentNode);

            if (currentNode.equals(smallNode)) {
                this.nodeId.set(currentNode);
                return;
            }

            CountDownLatch latch = new CountDownLatch(1);
            Stat stat = zk.exists(Objects.requireNonNull(preNode), new LockWatcher(latch));
            if (stat != null) {
                latch.await();
                nodeId.set(currentNode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * The function to unlock.
     * Simple way to delete the ZNode of current thread.
     */
    public void unlock() {
        try {
            if (null != nodeId) {
                zk.delete(nodeId.get(), -1);
            }
            Objects.requireNonNull(nodeId).remove();
        } catch (InterruptedException | KeeperException e) {
            LOGGER.error(e.getMessage(), e, e.getStackTrace());
        }
    }
}
