package experiment2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SoDistinctEvent {
	public static class SoDistinctEventMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text outEventId = new Text();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] e = value.toString().split("\\s+", 8);
			if (e == null) {
				return;
			}
			boolean flag = true;
			if (e.length > 4) {
				for (char c : e[4].toCharArray()) {
					if (!Character.isDigit(c)) {
						flag = false;
						break;
					}
				}

				if (flag) {
					outEventId.set(e[4]);
					context.write(outEventId, NullWritable.get());
				}
			}
		}
	}

	public static class SoDistinctEventReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			context.write(key, NullWritable.get());

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenTest <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow User SoDistinctEvent ");
		job.setJarByClass(SoDistinctEvent.class);
		job.setMapperClass(SoDistinctEventMapper.class);
		job.setReducerClass(SoDistinctEventReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
