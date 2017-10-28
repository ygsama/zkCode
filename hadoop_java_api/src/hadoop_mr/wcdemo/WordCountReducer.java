package hadoop_mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KEYIN, VALUEIN 对应着map输出的 KEYOUT, VALUEOUT
 * KEYOUT, VALUEOUT 对应着reduce输出的结果和数据类型
 * 
 * @author G
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * 输入的key是一组相同的单词键值对的key
	 * 输入的values是一组相同的单词键值对的‘值’的迭代器
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		context.write(key, new IntWritable(count));
		
	}
	
}
