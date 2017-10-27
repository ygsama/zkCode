package hadoop_rpc.service;

import hadoop_rpc.protocol.ClientNameNodeProtocol;

/**
 * 模拟NameNode
 * @author G
 *
 */
public class MyNameNode implements ClientNameNodeProtocol{

	/**
	 * 模拟namenode业务方法，查询元数据
	 * @return
	 */
	public String getMetaData(String path){
		
		
		return path+": 3 - {BLK_1,BLK_2} ....";
	}
}
