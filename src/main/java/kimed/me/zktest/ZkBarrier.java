package kimed.me.zktest;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZkBarrier {
	static final int size = 180;
	static final String root = "/barrier";
	ZooKeeper zk = null;//can remove static keyword,then create @size tcp connections to zk hosts.
	Integer mutex;
	String nodeName;

	ZkBarrier(String address, String nodeName) {
		this.nodeName = nodeName;
		mutex = new Integer(-1);
		if (zk == null) {
			try {
				zk = new ZooKeeper(address, 3000, new ZkWatcher());
				if (zk != null) {
					Stat s = zk.exists(root, true);
					if (s == null) {
						zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private class ZkWatcher implements Watcher {
		public void process(WatchedEvent event) {
			System.out.println(new Date().getTime() + ":get ZkWatcher watch even,path:" + event.getPath() + ",type:"
					+ event.getType() + ",state:" + event.getState());
		}
	}

	private class BarrierWatcher implements Watcher {
		public void process(WatchedEvent event) {
			System.out.println(new Date().getTime() + ":get BarrierWatcher watch even,path:" + event.getPath()
					+ ",type:" + event.getType() + ",state:" + event.getState());
			synchronized (mutex) {
				mutex.notify();
			}
		}
	}

	private boolean enter() {
		try {
			zk.create(root + "/" + nodeName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			BarrierWatcher bw = new BarrierWatcher();
			while (true) {
				synchronized (mutex) {
					List<String> childrenList = zk.getChildren(root, bw);
					if (childrenList.size() < size) {
						mutex.wait();
					} else {
						return true;
					}
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean leave() {
		try {
			zk.delete(root + "/" + nodeName, 0);

			synchronized (mutex) {
				while (true) {
					List<String> childrenList = zk.getChildren(root, new BarrierWatcher());
					if (childrenList.size() > 0) {
						mutex.wait();
					} else {
						return true;
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
		return false;
	}

	class ZkRunable implements Runnable {
		private ZkBarrier zkBarrier;

		ZkRunable(ZkBarrier zkBarrier) {
			this.zkBarrier = zkBarrier;
		}

		public void run() {
			if (zkBarrier.enter()) {
				System.out.println("node:" + nodeName + " get notify:all entered.");
				try {
					Thread.sleep(2 * 2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (zkBarrier.leave()) {
					System.out.println("node:" + nodeName + " get notify:all leaved.");
				}
			}
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		String hostAddress = "192.168.0.230:2181,192.168.0.231:2181,192.168.0.232:2181";
		int size = ZkBarrier.size;
		for (int i = 0; i < size; i++) {
			ZkBarrier zkBr = new ZkBarrier(hostAddress, "b" + i);
			Thread t = new Thread(zkBr.new ZkRunable(zkBr));
			t.start();
		}
	}
}
