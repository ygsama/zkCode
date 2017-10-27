package hadoop_rpc.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import hadoop_rpc.protocol.ClientNameNodeProtocol;
import hadoop_rpc.protocol.IUserLoginService;

/**
 * 模拟启动hdfs服务
 * @author G
 *
 */
public class PublishServiceUtil {

	public static void main(String[] args) throws Exception {
		
		Builder builder = new RPC.Builder(new Configuration());
		builder.setBindAddress("localhost")
			   .setPort(8888)
			   .setProtocol(ClientNameNodeProtocol.class)
			   .setInstance(new MyNameNode());
		Server server = builder.build();
		server.start();
		
		
		Builder builder2 = new RPC.Builder(new Configuration());
		builder2.setBindAddress("localhost")
		.setPort(9999)
		.setProtocol(IUserLoginService.class)
		.setInstance(new UserLoginServiceImpl());
		Server server2 = builder2.build();
		server2.start();
	}
}
