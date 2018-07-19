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

import TopTenTest.MRDPUtils;

public class CtripTop10Test {
	public static class Top10Mapper extends Mapper<Object, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] e = value.toString().split(",", 10);
			if(e!=null){
				repToRecordMap.put(Integer.parseInt(e[8]), new Text(value));
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class Top10Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text value : values) {
				String[] e = value.toString().split(",", 10);
				repToRecordMap.put(Integer.parseInt(e[8]), new Text(value));
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			for(Text t:repToRecordMap.descendingMap().values()){
				context.write(NullWritable.get(), t);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopKTest <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow appent TopKTest");
		job.setJarByClass(CtripTop10Test.class);
		job.setMapperClass(Top10Mapper.class);
		job.setReducerClass(Top10Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
