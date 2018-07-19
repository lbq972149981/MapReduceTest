package MP;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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


public class PartitioningExampleTest {
	public static class LastAccessDateMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");

			// Grab the last access date
			String strDate = strs[1];
			outkey.set(strDate);
			context.write(outkey, value);
		}
	}

	public static class LastAccessDatePartitioner extends Partitioner<Text, Text> implements Configurable {

		private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
		private Configuration conf = null;
		private int minLastAccessDateYear = 0;

		public int getPartition(Text key, Text value, int numPartitions) {
			return (Integer.parseInt(key.toString().substring(0,4)) - minLastAccessDateYear);
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

	public static class ValueReducer extends Reducer<Text, Text, Text, NullWritable> {

		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MP.PartitioningExampleTest");
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2014);
		// Last access dates span between 2008-2011, or 4 years
		job.setNumReduceTasks(2);
		job.setJarByClass(PartitioningExampleTest.class);
		job.setMapperClass(LastAccessDateMapper.class);
		job.setReducerClass(ValueReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		String inputPath = "input/pro_quantity";
		String outputPath = "output/";
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// 判断输出文件是否存在
		Path output = new Path(outputPath);
		FileSystem fileSystem = output.getFileSystem(conf);
		if(fileSystem.exists(output)){
			fileSystem.delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
