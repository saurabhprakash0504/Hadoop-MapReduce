package PackageDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;*/
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static void main(String[] args) throws Exception {
	    Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		FileSystem fs=FileSystem.get(c);
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
			
		Job j = new Job(c, "wordcount");
		j.setJarByClass(WordCount.class);
		j.setMapperClass(MapForWordCount.class);
		//j.setCombinerClass(ReduceForWordCount.class);
		j.setNumReduceTasks(2);
		j.setPartitionerClass(PartitionerForWordCounts.class);
		j.setNumReduceTasks(2);
		j.setReducerClass(ReduceForWordCount.class);
		j.setNumReduceTasks(2);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");
			for (String word : words) {
				Text outputKey = new Text(word.toUpperCase().trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
		}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con)throws IOException, InterruptedException {
			System.out.println("in reducer ");
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
	
	
	public static class PartitionerForWordCounts extends Partitioner<Text, IntWritable>{
		
		@Override
		public int getPartition(Text arg0, IntWritable arg1, int numReduceTasks) {
			String text=arg0.toString();
			System.out.println("intValue >>> "+text +":::::::;"+numReduceTasks);
	         
			int partition = 0;
			
			if(text.length()==3)
	         {
	        	 partition = 0%numReduceTasks;
	         }
	         else if(text.length()==5)
	         {
	        	 partition = 1%numReduceTasks;
	         }
	         else
	        	 partition = 2%numReduceTasks;
		
	         System.out.println("partition:" + partition);
	         return partition;
		
		}
		
	}
}