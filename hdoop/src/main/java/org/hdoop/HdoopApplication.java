package org.hdoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class HdoopApplication {

	public static void main(String[] args) {
		SpringApplication.run(HdoopApplication.class, args);
		try {
//			if (args.length != 2) {
//				System.out.printf("Usage: HdoopApplication <input dir> <output dir>\n");
//				System.exit(-1);
//			}			
			Configuration conf = new Configuration();
			Job job= Job.getInstance(conf, "WORD_COUNT");
			job.setJarByClass(HdoopApplication.class);
			job.setMapperClass(StubMapper.class);
			job.setReducerClass(StubReducer.class);
			//job.setCombinerClass(Reducers.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath(job, new Path("/user/ramubhumireddy7554/hdin/htusers.txt"));
			FileOutputFormat.setOutputPath(job, new Path("/user/ramubhumireddy7554/hdop/" + System.currentTimeMillis() + "/"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Bean
	public StubMapper getMapper() {
		return new StubMapper();
	}
	
	@Bean
	public StubReducer getReducer() {
		return new StubReducer();
	}
}
