package com.sample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SixthListOfMovieIdWithMAXminAVG {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job job = new Job(configuration, "movieRating");
		job.setJarByClass(SixthListOfMovieIdWithMAXminAVG.class);
		job.setMapperClass(MapperMovieRating.class);
		job.setCombinerClass(ReducerMovieRating.class);
		job.setPartitionerClass(PartitionerMovieRating.class);
		job.setNumReduceTasks(4);
		job.setReducerClass(ReducerMovieRating.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	static class MapperMovieRating extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\t");
			String movie = words[0];
			String movieRating = words[2];
			IntWritable outKey = new IntWritable(Integer.parseInt(movie));
			DoubleWritable outValue = new DoubleWritable(movieRating.length() > 0 ? Double.parseDouble(movieRating) : 0);
			con.write(new Text(outKey.toString()), outValue);
		}
	}

	static class ReducerMovieRating extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
				throws IOException, InterruptedException {
			double avg = 0.0;
			double sum=0;
			int count=0;
			double max = Double.MIN_VALUE;
			double min = Double.MAX_VALUE;
			for (DoubleWritable value : values) {
				max = (value.get() > max) ? value.get() : max;
				min = value.get() < min ? value.get() : min;
				sum=sum+value.get();
				count=count+1;
			}
			avg=sum/count;
			con.write(key, new DoubleWritable(max));
			con.write(key, new DoubleWritable(min));
			con.write(key, new DoubleWritable(avg));
			
			
		}
	}

	static class PartitionerMovieRating extends Partitioner<Text, DoubleWritable> {
		
		
		@Override
		public int getPartition(Text arg0, DoubleWritable arg1, int arg2) {
			if(arg0.toString().startsWith("max"))
				return 0 % arg2;
			else if(arg0.toString().startsWith("min"))
				return 1 % arg2;
			else if(arg0.toString().startsWith("avg"))
				return 2 % arg2;
			return 3 % arg2;
		}

	}

}
