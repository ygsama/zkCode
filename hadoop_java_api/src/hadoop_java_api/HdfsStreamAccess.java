package hadoop_java_api;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 用留的方式读取hdfs文件
 * 可以读取指定偏移量范围的数据
 * @author G
 *
 */
public class HdfsStreamAccess {

	FileSystem fs = null;
	Configuration conf = null;
	
	@Before
	public void init() throws Exception {
		
		conf = new Configuration();
		// 拿到操作文件系统的客户端实例对象
//		fs = FileSystem.get(conf);
		// 直接传入URI和用户身份
		fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
	}
	
	
	/**
	 * 通过流的方式上传文件到hdfs
	 * @throws Exception
	 */
	@Test
	public void testUpload() throws Exception {

		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/student") ,true);
		FileInputStream fileInputStream = new FileInputStream("D:/student.html");
		IOUtils.copy(fileInputStream, fsDataOutputStream);
	}
	
	/**
	 * 通过流的方式从hdfs下载文件
	 * @throws Exception
	 */
	@Test
	public void testDownload() throws Exception {
		
		FSDataInputStream fsDataInputStream = fs.open(new Path("/student"));
		FileOutputStream fileOutputStream = new FileOutputStream("D:/student.html.bak");
		IOUtils.copy(fsDataInputStream, fileOutputStream);
	}
	
	/**
	 * 带偏移量的流下载
	 * @throws Exception
	 */
	@Test
	public void testRandomAceess() throws Exception{
		
		FSDataInputStream fsDataInputStream = fs.open(new Path("/student"));
		fsDataInputStream.seek(12);
		FileOutputStream fileOutputStream = new FileOutputStream("D:/student.html.bak");
		IOUtils.copy(fsDataInputStream, fileOutputStream);
	}
	
	/**
	 * 显示hdfs文件内容到控制台
	 * @throws Exception
	 */
	@Test
	public void testCat() throws Exception{
		
		FSDataInputStream fsDataInputStream = fs.open(new Path("/student"));
		IOUtils.copy(fsDataInputStream, System.out);
	}
	
	
	
}
