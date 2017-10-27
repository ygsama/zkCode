package hadoop_rpc.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import hadoop_rpc.protocol.ClientNameNodeProtocol;

/**
 * 模拟hdfs客户端
 * @author G
 *
 */
public class MyHDFSClient {

	public static void main(String[] args) throws Exception {
		ClientNameNodeProtocol namenode = RPC.getProxy(ClientNameNodeProtocol.class, 1L,
				new InetSocketAddress("localhost", 8888), new Configuration());
		String metaData = namenode.getMetaData("/student");
		System.out.println(metaData);
	}
}
