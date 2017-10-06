package server;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 服务端-接收客户端的请求数据，处理后再发给客户端
 * @author G
 *
 */
public class ServiceServer {

	public static void main(String[] args) throws Exception {
		// 创建一个serversocket，绑定到8878端口
		ServerSocket socket = new ServerSocket();
		socket.bind(new InetSocketAddress("localhost", 8878));
		
		// 接收客户端请求
		while (true) {
			Socket accept = socket.accept();
			new Thread(new ServiceServerTask(accept)).start();
		}
		
		
	}
	
}
