package hadoop_mr.wcdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于yarn的客户端
 * 需要在此封装mr程序的的运行配置参数,指定jar包
 * 最后提交给yarn
 * @author G
 *
 */
public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "master");
		Job job = Job.getInstance(conf);
		
//		job.setJar("/home/hadoop/xx.jar");
		job.setJarByClass(WordCountDriver.class);
		
		// 指定本次Job要执行的业务类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 指定mapper输出的键值对数据类型
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		
		// 指定最终输出的键值对的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// job输入文件的所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		// job输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// job配置的参数和相关jar包提交给yarn运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		
	}
}
