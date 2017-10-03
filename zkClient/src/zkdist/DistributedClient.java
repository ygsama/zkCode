package zkdist;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 使用zookeeper动态感知服务器上下线-客户端
 * @author G
 *
 */
public class DistributedClient {

	private static final String connStr = "master:2181,hadoop1:2181,hadoop2:2181";
	private static final int sessionTimeout = 2000;
	private static final String parentNode = "/servers";
	private volatile List<String> serverList;
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
				try {
					//　重新更新服务器列表，并且注册监听
					getServerList();
				} catch (Exception e) {
				}
			}
		});
		
		Thread.sleep(2000);
	}
	
	/**
	 * 获取服务器信息列表
	 * @throws Exception
	 */
	public void getServerList() throws Exception {
		
		/**
		 * 获取服务器子节点信息，监听父节点
		 */
		List<String> children = zk.getChildren(parentNode, true);
		// 局部list变量存服务信息
		List<String> servers = new ArrayList<String>();
		for (String child : children) {
			// child子节点名
			byte[] data = zk.getData(parentNode+"/"+child, false, null);
			servers.add(new String(data));
		}
		// 把servers赋值给成员变量,提供给其他业务使用
		serverList= servers;
		
		// 打印服务器列表
		for (String string : servers) {
			System.out.println(serverList.toString());
		}
	}
	
	/**
	 * 业务功能
	 * @throws Exception 
	 */
	private void handleBussiness() throws Exception {
		System.out.println("client start working");
		Thread.sleep(Long.MAX_VALUE);
		
	}
	
	public static void main(String[] args) throws Exception {
		
		// 获取zk连接
		DistributedClient client = new DistributedClient();
		client.getConnction();
		// 获取servers的子节点信息并监听，获取服务器列表
		client.getServerList();
		// 业务线程启动
		client.handleBussiness();
	}
}
