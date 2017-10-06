package server;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * 服务端-接收后的业务逻辑处理
 * @author G
 *
 */
public class ServiceServerTask implements Runnable{

	Socket socket = null ;
	InputStream inputStream = null;
	OutputStream outputStream = null;
	
	public ServiceServerTask(Socket accept) {
		this.socket = accept;
		
	}

	// 跟客戶端交互
	@Override
	public void run() {
		try {
			// 获取输入输出流
			inputStream = socket.getInputStream();
			outputStream = socket.getOutputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			
			// 从流中读，阻塞读取
			String line = null;
			GetDataServiceImpl serviceImpl = new GetDataServiceImpl();
			while ((line = reader.readLine())!=null) {
				String result = serviceImpl.getData(line);
				
				// 将结果写入输出流发给客户端
				PrintWriter printWriter = new PrintWriter(outputStream);
				printWriter.println(result);
				printWriter.flush();
				System.out.println("send ok - "+result);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				inputStream.close();
				outputStream.close();
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
	}


	
}
