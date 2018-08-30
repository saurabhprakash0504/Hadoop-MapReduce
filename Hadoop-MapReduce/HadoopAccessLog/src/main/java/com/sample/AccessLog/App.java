package com.sample.AccessLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class App {
	static List<IPAddress> list = new ArrayList();

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();

		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		FileSystem fs = FileSystem.get(c);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		Job j = new Job(c, "App");
		j.setJarByClass(App.class);
		j.setMapperClass(MapForWordCounts.class);
		j.setReducerClass(ReduceForWordCounts.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForWordCounts extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" - - ");
			IntWritable outputValue = new IntWritable(1);
			con.write(new Text(words[0]), outputValue);
		}
	}

	public static class ReduceForWordCounts extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			if (list.size() > 10) {
				
				IPAddress first=list.get(0);
				if (first.count < sum) {
					list.remove(0);
					list.add(new IPAddress(word.toString(), sum));
					Collections.sort(list);
				}
			} else {
				list.add(new IPAddress(word.toString(), sum));
				// treeMap.put(word.toString(), sum);
				Collections.sort(list);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(IPAddress a:list) {
				System.out.println(a.ip+" >>> "+a.count);
			}
		}
	}
}
