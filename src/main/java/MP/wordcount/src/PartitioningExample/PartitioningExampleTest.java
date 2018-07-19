package PartitioningExample;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import TopTenTest.MRDPUtils;

public class PartitioningExampleTest {
	public static class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text> {

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		private IntWritable outkey = new IntWritable();

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			// Grab the last access date
			String strDate = parsed.get("LastAccessDate");
			// Parse the string into a Calendar object
			Calendar cal = Calendar.getInstance();
			try {
				if (strDate != null) {
					cal.setTime(frmt.parse(strDate));
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			outkey.set(cal.get(Calendar.YEAR));
			context.write(outkey, value);
		}
	}

	public static class LastAccessDatePartitioner extends Partitioner<IntWritable, Text> implements Configurable {

		private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
		private Configuration conf = null;
		private int minLastAccessDateYear = 0;

		public int getPartition(IntWritable key, Text value, int numPartitions) {
			return (key.get() - minLastAccessDateYear)/4;
		}

		public Configuration getConf() {
			return conf;
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
			minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
		}

		public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
			job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
		}
	}

	public static class ValueReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PartitioningExampleTest");
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2014);
		// Last access dates span between 2008-2011, or 4 years
		job.setNumReduceTasks(1);
		job.setJarByClass(PartitioningExampleTest.class);
		job.setMapperClass(LastAccessDateMapper.class);
		job.setReducerClass(ValueReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
