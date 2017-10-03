package zkdist;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 使用zookeeper动态感知服务器上下线-服务端
 * @author G
 *
 */
public class DistributedServer {

	private static final String connStr = "master:2181,hadoop1:2181,hadoop2:2181";
	private static final int sessionTimeout = 2000;
	private static final String parentNode = "/servers";
	private ZooKeeper zk = null;
	
	/**
	 * 到集群的客户端连接
	 * @throws Exception
	 */
	private void getConnction() throws Exception {
		zk = new ZooKeeper(connStr, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 收到事件通知后的回调函数，事件处理逻辑
				System.out.println(event.getType()+"\t"+event.getPath()+"\tconnected");
			}
		});
	}
	/**
	 * 向集群注册服务器信息
	 * @param hostname
	 * @throws Exception
	 */
	private void registerServer(String hostname) throws Exception {
		String create = zk.create(parentNode+"/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname+"is online "+hostname);
	}
	
	/**
	 * 业务功能
	 * @throws Exception 
	 */
	private void handleBussiness(String hostname) throws Exception {
		System.out.println(hostname+" start working");
		Thread.sleep(Long.MAX_VALUE);
		
	}
	
	public static void main(String[] args) throws Exception {
		
		// 获取连接
		DistributedServer server = new DistributedServer();
		server.getConnction();
		Thread.sleep(1000);
		// 注册服务器信息
		server.registerServer(args[0]);
		
		// 业务功能
		server.handleBussiness(args[0]);
		
	}
}
