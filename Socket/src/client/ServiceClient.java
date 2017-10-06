package client;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

/**
 * 客户端-不断发出请求数据，获得服务端返回的处理结果
 * @author G
 *
 */
public class ServiceClient {

	
	public static void main(String[] args) throws Exception {
	
		// 建立连接
		Socket socket = new Socket("localhost",8878);
		
		// 获取输入输出流
		InputStream inputStream = socket.getInputStream(); 
		OutputStream outputStream = socket.getOutputStream();
		
		PrintWriter printWriter = new PrintWriter(outputStream);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
		
		Scanner scanner = new Scanner(System.in);
		String nextLine = null;
		String line = null;
		
		while (true) {
			nextLine = scanner.nextLine();
			printWriter.println(nextLine);
			printWriter.flush();
			nextLine = null;
			
//			while ((line = bufferedReader.readLine())!=null) {
//				System.out.println("accept - "+line);
//			}
			line = bufferedReader.readLine();
			System.out.println("accept - "+line);
			
		}
//		inputStream.close();
//		outputStream.close();
//		socket.close();
	}
}
