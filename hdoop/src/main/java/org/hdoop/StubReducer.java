package org.hdoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

@Component
public class StubReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)	throws IOException, InterruptedException {

//		IntWritable result = new IntWritable();
//		int sum = 0;
//		for (IntWritable val : values) {
//			sum += val.get();
//		}
//		result.set(sum);
//		context.write(key, result);

		long sum = 0;
		for (LongWritable iw : values) {
			sum += iw.get();
		}
		context.write(key, new LongWritable(sum));
	}

}








//Reducer<Text, LongWritable, Text, LongWritable> {
//
//	@Override
//	protected void reduce(Text key, Iterable<LongWritable> values, Context context)	throws IOException, InterruptedException {
//
////		IntWritable result = new IntWritable();
////		int sum = 0;
////		for (IntWritable val : values) {
////			sum += val.get();
////		}
////		result.set(sum);
////		context.write(key, result);
//
//		long sum = 0;
//		for (LongWritable iw : values) {
//			sum += iw.get();
//		}
//		context.write(key, new LongWritable(sum));
//	}
//
//}
