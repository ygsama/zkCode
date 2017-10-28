package hadoop_mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 对文本中的单词计数，map程序
 * 
 * KEYIN	一行文本的起始偏移量,Long; 在Hadoop中有封装好的类型接口LongWritable
 * VALUEIN	一行文本的内容，String; 同上,使用Text
 * 
 * KEYOUT	用户自定义map逻辑处理完毕后输出的Key,输出单词则是String
 * VALUEOUT	用户自定义map逻辑处理完毕后输出的Value，输出次数则是Integer
 * @author G
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * map阶段的业务逻辑写在重载的map方法中
	 * maptask会对每一行调用map方法
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 获取每一行的字符串
		String line = value.toString();
		// 切割
		String[] words = line.split(" ");
		
		// map输出为<word,count>
		for (String word : words) {
			context.write(new Text(word),new IntWritable(1));
		}
		
		
	}
	
}
