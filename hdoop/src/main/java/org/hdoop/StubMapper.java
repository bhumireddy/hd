package org.hdoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

@Component
public class StubMapper extends Mapper<Object, Text, Text, LongWritable> {

	//private final static IntWritable one = new IntWritable(1);
 
	@Override
	protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException {
//		StringTokenizer itr = new StringTokenizer(value.toString());
//		while (itr.hasMoreTokens()) {
//			Text word = new Text();
//			word.set(itr.nextToken());
//			context.write(word,  new IntWritable(1));
//		}
		String[] words = value.toString().split(" ");
		for (String word : words) {
			context.write(new Text(word),  new LongWritable(1));
		}
	}

}
 



//Mapper<Object, Text, Text, LongWritable> {
//
//	//private final static IntWritable one = new IntWritable(1);
//
//	@Override
//	protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException {
////		StringTokenizer itr = new StringTokenizer(value.toString());
////		while (itr.hasMoreTokens()) {
////			Text word = new Text();
////			word.set(itr.nextToken());
////			context.write(word,  new IntWritable(1));
////		}
//		String[] words = value.toString().split(" ");
//		for (String word : words) {
//			context.write(new Text(word),  new LongWritable(1));
//		}
//	}
//
//}
