package com.sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ThirdListOfMovieIDRatedAtleastOnce {

		public static void main(String[] args) throws Exception {
			Configuration configuration = new Configuration();
			String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
			Path input = new Path(files[0]);
			Path output = new Path(files[1]);
			Job job = new Job(configuration, "movieRating");
			job.setJarByClass(ThirdListOfMovieIDRatedAtleastOnce.class);
			job.setMapperClass(MapperMovieRating.class);
			//job.setCombinerClass(ReducerMovieRating.class);
			// job.setPartitionerClass(PartitionerMovieRating.class);
			// job.setNumReduceTasks(3);
			//job.setReducerClass(ReducerMovieRating.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}

		static class MapperMovieRating extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
			public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
				String line = value.toString();
				String[] words = line.split("\t");
				String word = words[2];
				if(null==word||word.isEmpty()) {
					IntWritable outKey = new IntWritable(Integer.parseInt(words[0]));
					con.write(outKey,NullWritable.get());
				}
			}
		}

		/*static class ReducerMovieRating extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
			public void reduce(IntWritable key, Iterable<IntWritable> values, Context con)
					throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable value : values) {
					sum = sum + value.get();
				}
				con.write(key, new NullWritable());
			}
		}*/

		/*
		 * static class PartitionerMovieRating extends Partitioner<IntWritable,
		 * IntWritable>{
		 * 
		 * @Override public int getPartition(IntWritable arg0, IntWritable arg1, int
		 * arg2) { return 0; }
		 * 
		 * }
		 */
	}


