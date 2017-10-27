package hadoop_java_api;

import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 
 * 也可以在构造客户端fs对象时，通过参数传递进去
 * @author G
 *
 */
public class HDFSClientDemo {

	FileSystem fs = null;
	Configuration conf = null;
	
	@Before
	public void init() throws Exception {
		
		conf = new Configuration();
		conf.set("fs.DefaultFS", "hdfs://master:9000");
		// 拿到操作hdfs的客户端实例对象
		fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
		
	}
	
	/**
	 * 上传
	 * @throws Exception
	 */
	@Test
	public void testUpload() throws Exception{

		fs.copyFromLocalFile(new Path("e:/taotao.sql"), new Path("/taotao.sql.bak"));
		fs.close();
	}
	
	/**
	 * 下载
	 * @throws Exception
	 */
	@Test
	public void testDownLoad() throws Exception{
		
		fs.copyToLocalFile(new Path("/taotao.sql.bak"), new Path("e:/taotao1.sql"));
		fs.close();
	}
	
	/**
	 * 打印配置文件
	 */
	@Test
	public void testConf(){
		Iterator<Entry<String, String>> iterator =  conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getKey() + "--" + entry.getValue());//conf加载的内容
		}
	}
	
	/**
	 * 创建目录
	 */
	@Test
	public void makdirTest() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
		System.out.println(mkdirs);
	}
	
	/**
	 * 删除
	 */
	@Test
	public void deleteTest() throws Exception{
		boolean delete = fs.delete(new Path("/aaa"), true);//true， 递归删除
		System.out.println(delete);
	}
	
	/**
	 * 打印文件列表
	 * @throws Exception
	 */
	@Test
	public void listTest() throws Exception{
		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.err.println(fileStatus.getPath()+"================="+fileStatus.toString());
		}
		//会递归找到所有的文件
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			String name = next.getPath().getName();
			Path path = next.getPath();
			System.out.println(name + "|------------------|" + path.toString());
		}
	}
	
	
	/**
	 * 递归列出指定目录下所有子文件夹中的文件
	 * @throws Exception
	 */
	@Test
	public void testLs() throws Exception {
		
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()){
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println("blocksize: " +fileStatus.getBlockSize());
			System.out.println("owner: " +fileStatus.getOwner());
			System.out.println("Replication: " +fileStatus.getReplication());
			System.out.println("Permission: " +fileStatus.getPermission());
			System.out.println("Name: " +fileStatus.getPath().getName());
			System.out.println("------------------");
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for(BlockLocation b:blockLocations){
				System.out.println("块起始偏移量: " +b.getOffset());
				System.out.println("块长度:" + b.getLength());
				//块所在的datanode节点
				String[] datanodes = b.getHosts();
				for(String dn:datanodes){
				System.out.println("datanode:" + dn);
				}
			}
		}
	}
	
	
//	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://master:9000");
//		//拿到一个文件系统操作的客户端实例对象
//		FileSystem fs = FileSystem.get(conf);
//		
//		fs.copyFromLocalFile(new Path("G:/access.log"), new Path("/access.log.copy"));
//		fs.close();
//	}
	
}
