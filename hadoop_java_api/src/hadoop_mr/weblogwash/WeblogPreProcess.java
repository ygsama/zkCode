package hadoop_mr.weblogwash;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 清洗日志，并转换时间格式
 * @author G
 *
 */
public class WeblogPreProcess {

	static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Text k = new Text();
		NullWritable v = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			// 使用工具类处理，获得封装的日志Bean
			WebLogBean webLogBean = WebLogParser.parser(line);
			//可以插入一个静态资源过滤（.....）
			/*WebLogParser.filterStaticResource(webLogBean);*/
			if (!webLogBean.isValid())
				return;
			k.set(webLogBean.toString());
			context.write(k, v);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(WeblogPreProcess.class);

		job.setMapperClass(WeblogPreProcessMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("C:/wordcount/weblog/input"));
		FileOutputFormat.setOutputPath(job, new Path("C:/wordcount/weblog/output"));

		job.waitForCompletion(true);

	}
}
