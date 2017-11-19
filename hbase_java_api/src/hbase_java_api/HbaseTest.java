package hbase_java_api;


import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest {

	/**
	 * 配置ss
	 */
	public static Configuration config = null;
	public Connection connection = null;
	public Table table = null;

	@Before
	public void init() throws Exception {
		config = HBaseConfiguration.create();// 配置
		config.set("hbase.zookeeper.quorum", "master,hadoop1,hadoop2");// zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("test1"));
	}

	/**
	 * 创建表
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// 创建表管理类
		HBaseAdmin admin =  (HBaseAdmin) connection.getAdmin(); // new HBaseAdmin(config); // hbase表管理
		// 创建表
		TableName tableName = TableName.valueOf("test1"); // 表名称
		HTableDescriptor desc = new HTableDescriptor(tableName);
		// 创建列族
		HColumnDescriptor family = new HColumnDescriptor("info"); // 列族
		HColumnDescriptor family2 = new HColumnDescriptor("info2"); // 列族
		// 将列族添加到表中
		desc.addFamily(family);
		desc.addFamily(family2);
		// 创建表
		admin.createTable(desc); // 创建表
	}

	/**
	 * 删除表
	 * @throws Exception
	 */
	@Test
	public void deleteTable() throws Exception {
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin(); //new HBaseAdmin(config);
		admin.disableTable("test3");
		admin.deleteTable("test3");
		admin.close();
	}

	/**
	 * 向hbase中增加数据
	 * 只能加列，不能加行，加列操作等于修改
	 * @throws Exception
	 */
	@Test
	public void insertData() throws Exception {
		ArrayList<Put> arrayList = new ArrayList<Put>();
		Put put = new Put(Bytes.toBytes("171114"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name1"), Bytes.toBytes("zhang"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password1"), Bytes.toBytes(1234));
		arrayList.add(put);
		
		//插入数据
		table.put(arrayList);
		//提交
//		table.flushCommits();
	}

	/**
	 * 修改数据
	 * @throws Exception
	 */
	@Test
	public void uodateData() throws Exception {
		Put put = new Put(Bytes.toBytes("1234"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("namessss"), Bytes.toBytes("lisi1234"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(1234));
		//插入数据
		table.put(put);
		//提交
//		table.flushCommits();
	}

	/**
	 * 删除数据
	 * @throws Exception
	 */
	@Test
	public void deleteDate() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("171114"));
		delete.addFamily(Bytes.toBytes("info2"));
		table.delete(delete);
//		table.flushCommits();
	}

	/**
	 * 单条查询
	 * @throws Exception
	 */
	@Test
	public void queryData() throws Exception {
		Get get = new Get(Bytes.toBytes("171114"));
		Result result = table.get(get);
		System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"))));
	}

	/**
	 * 全表扫描
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("info"));
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
		scan.setStartRow(Bytes.toBytes("0"));
		scan.setStopRow(Bytes.toBytes("171119"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.print(Bytes.toString(result.getRow()) + ";");
			System.out.print(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))) + ";");
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))) + ";");
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}
	}

	/**
	 * 全表扫描的过滤器
	 * 列值过滤器
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter1() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		//过滤器：列值过滤器
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
				Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
				Bytes.toBytes("zhangsan2"));
		// 设置过滤器
		scan.setFilter(filter);

		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}

	}
	/**
	 * rowkey过滤器
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter2() throws Exception {
		
		// 创建全表扫描的scan
		Scan scan = new Scan();
		//匹配rowkey以wangsenfeng开头的
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^12341"));
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}

		
	}
	
	/**
	 * 匹配列名前缀
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter3() throws Exception {
		
		// 创建全表扫描的scan
		Scan scan = new Scan();
		//匹配rowkey以wangsenfeng开头的
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("na"));
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println("info:name："
					+ Bytes.toString(result.getValue(Bytes.toBytes("info"),
							Bytes.toBytes("name"))));
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
				System.out.println("info:age："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info"),
								Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
				System.out.println("infi:sex："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info"),
								Bytes.toBytes("sex"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
				System.out
				.println("info2:name："
						+ Bytes.toString(result.getValue(
								Bytes.toBytes("info2"),
								Bytes.toBytes("name"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
				System.out.println("info2:age："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
								Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
				System.out.println("info2:sex："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
								Bytes.toBytes("sex"))));
			}
		}
		
	}
	/**
	 * 过滤器集合
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter4() throws Exception {
		
		// 创建全表扫描的scan
		Scan scan = new Scan();
		//过滤器集合：MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
		FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
		//匹配rowkey以wangsenfeng开头的
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^wangsenfeng"));
		//匹配name的值等于wangsenfeng
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"),
				Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
				Bytes.toBytes("zhangsan"));
		filterList.addFilter(filter);
		filterList.addFilter(filter2);
		// 设置过滤器
		scan.setFilter(filterList);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println("info:name："
					+ Bytes.toString(result.getValue(Bytes.toBytes("info"),
							Bytes.toBytes("name"))));
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
				System.out.println("info:age："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info"),
								Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
				System.out.println("infi:sex："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info"),
								Bytes.toBytes("sex"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
				System.out
				.println("info2:name："
						+ Bytes.toString(result.getValue(
								Bytes.toBytes("info2"),
								Bytes.toBytes("name"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
				System.out.println("info2:age："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
								Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
				System.out.println("info2:sex："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
								Bytes.toBytes("sex"))));
			}
		}
		
	}

	@After
	public void close() throws Exception {
		table.close();
		connection.close();
	}

}