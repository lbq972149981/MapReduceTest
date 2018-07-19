package SoDistinctUserTest;

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


public class SoDistinctUser {
	public static class SoDistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text outUserId = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String userId = parsed.get("UserId");
			if(userId==null){
				return;
			}
			outUserId.set(userId);
			context.write(outUserId,NullWritable.get() );

		}
	}

	public static class SoDistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
				context.write(key,NullWritable.get());
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenTest <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow User SoDistinctUser ");
		job.setJarByClass(SoDistinctUser.class);
		job.setMapperClass(SoDistinctUserMapper.class);
		job.setReducerClass(SoDistinctUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
