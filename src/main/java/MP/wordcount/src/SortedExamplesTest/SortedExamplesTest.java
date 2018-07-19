package SortedExamplesTest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import PartitioningExample.PartitioningExampleTest;
import PartitioningExample.PartitioningExampleTest.LastAccessDateMapper;
import PartitioningExample.PartitioningExampleTest.LastAccessDatePartitioner;
import PartitioningExample.PartitioningExampleTest.ValueReducer;
import TopTenTest.MRDPUtils;

public class SortedExamplesTest {
	public static class LastAccessDateMapper extends Mapper<Object, Text, Text, Text> {

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		private Text outkey = new Text();

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			// Grab the last access date
			String strDate = parsed.get("LastAccessDate");
			// Parse the string into a Calendar object
			
			outkey.set(strDate);
			context.write(outkey, value);
		}
	}
	public static class ValueReducer extends Reducer<Text, Text, Text, NullWritable> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PartitioningExampleTest");
		Path inputPath = new Path(args[0]);
		Path partitionfile = new Path(args[1]+"_partitions.1st");
		Path outputStage = new Path(args[1]+"_staging");
		Path outputOrder = new Path(args[1]);
		
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2014);
		job.setNumReduceTasks(1);
		job.setJarByClass(PartitioningExampleTest.class);
		job.setMapperClass(LastAccessDateMapper.class);
		job.setReducerClass(ValueReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job,inputPath);
		FileOutputFormat.setOutputPath(job, outputOrder);
		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
