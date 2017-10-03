package zk;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class SimpleZkClient {

	private static final String connStr = "master:2181,hadoop1:2181,hadoop2:2181";
	private static final int sessionTimeout = 2000;
	private ZooKeeper client = null;

	@Before
	public void init() throws Exception{
		client = new ZooKeeper(connStr, sessionTimeout, new Watcher() {
					
					@Override
					public void process(WatchedEvent event) {
						// 收到事件通知后的回调函数，事件处理逻辑
						System.out.println(event.getType()+"\t"+event.getPath()+"\tconnected");
					}
				});
		Thread.sleep(1000);
	}
	
	/**
	 *  数据增删改查 = 路径，数据，权限，类型
	 */
	@Test
	public void testCreate() throws Exception{
		
		String nodePath = client.create("/servers", "hello zk".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(nodePath);
	}
	
	@Test
	public void getChildren() throws Exception {
		
		List<String> children = client.getChildren("/", true);
		for (String child : children) {
			System.out.println(child);
		}
	}
	
	@Test
	public void isExist() throws Exception{

		Stat stat = client.exists("/eclipse", false);
		System.out.println(stat==null?"not exist":"exist and data length is " + stat.getDataLength());
	}
	
	@Test
	public void getData() throws Exception{

		byte[] data = client.getData("/eclipse", false, null);
		System.out.println(new String(data));
	}
	
	@Test
	public void deleteZndoe() throws Exception{
		
		client.delete("/servers/server0000000000", -1);
	}
	
	@Test
	public void resetZndoe() throws Exception{
		
		client.setData("/eclipse", "hello world".getBytes(), -1);
		
	}
}
