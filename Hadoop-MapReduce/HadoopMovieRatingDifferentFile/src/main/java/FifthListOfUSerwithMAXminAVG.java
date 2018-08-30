
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FifthListOfUSerwithMAXminAVG {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);

		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		Job job = new Job(configuration, "movieRating");
		job.setJarByClass(FifthListOfUSerwithMAXminAVG.class);
		job.setMapperClass(MapperMovieRating.class);
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
			String user = words[1];
			String movieRating = words[2];
			DoubleWritable dvalue = new DoubleWritable(movieRating.length() > 0 ? Double.parseDouble(movieRating) : 0);

			con.write(new Text("max" + " " + user), dvalue);
			con.write(new Text("min" + " " + user), dvalue);
			con.write(new Text("avg" + " " + user), dvalue);
		}
	}

	static class ReducerMovieRating extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
				throws IOException, InterruptedException {
			double avg = 0.0;
			double sum = 0;
			int count = 0;
			double max = Double.MIN_VALUE;
			double min = Double.MAX_VALUE;
			for (DoubleWritable value : values) {
				max = (value.get() > max) ? value.get() : max;
				min = (value.get() < min) ? value.get() : min;
				sum = sum + value.get();
				count = count + 1;
			}
			System.out.println("key >> " + key + " count >> " + count);
			avg = sum / count;
			if (con.getTaskAttemptID().getTaskID().getId() == 0)
				con.write(key, new DoubleWritable(max));
			if (con.getTaskAttemptID().getTaskID().getId() == 2)
				con.write(key, new DoubleWritable(avg));
			if (con.getTaskAttemptID().getTaskID().getId() == 1)
				con.write(key, new DoubleWritable(min));
		}
	}

	static class PartitionerMovieRating extends Partitioner<Text, DoubleWritable> {
		@Override
		public int getPartition(Text arg0, DoubleWritable arg1, int arg2) {
			System.out.println("partitioner starts **************************************************" + arg0);
			String[] arrays = arg0.toString().split(" ");
			if (arrays[0].startsWith("max"))
				return 0 % arg2;
			else if (arrays[0].startsWith("min"))
				return 1 % arg2;
			else if (arrays[0].startsWith("avg"))
				return 2 % arg2;
			else
				return 3 % arg2;

		}
	}
}
