package MedianStdDevDriver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MedianStdDevDriver {
	public static class SOMedianStdDevMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] e = value.toString().split(",", 10);
			context.write(new Text(e[0]), new Text(e[8]));
		}
	}

	public static class SOMedianStdDevReducer extends Reducer<Text,Text,Text,Text> {
		private ArrayList<Float> CtripLengths = new ArrayList<Float>();
		private float StdDev = 0;
		private float Median = 0;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			float count = 0;
			CtripLengths.clear();
			StdDev = 0;
			// Iterate through all input values for this key
			for (Text val : values) {
				CtripLengths.add(Float.parseFloat(val.toString()));
				sum += Float.parseFloat(val.toString());
				count+=1.0f;
			}
			// sort commentLengths to calculate median
			Collections.sort(CtripLengths);
			// if commentLengths is an even value, average middle two elements
			if (count % 2 == 0) {
				Median= (CtripLengths.get((int) count / 2 - 1) + CtripLengths.get((int) count / 2)) / 2.0f;
			} else {
				// else, set median to middle value
				Median = CtripLengths.get((int) count / 2);
			}

			// calculate standard deviation
			float mean = sum / count;

			float sumOfSquares = 0.0f;
			for (Float f : CtripLengths) {
				sumOfSquares += (f - mean) * (f - mean);
			}
			StdDev = (float) Math.sqrt(sumOfSquares / (count - 1));
			System.out.println(StdDev);
			context.write(key,new Text(String.valueOf(Median)));
			context.write(key,new Text(String.valueOf(StdDev)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MedianStdDevDriver <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow Comment Length Median StdDev By Hour");
		job.setJarByClass(MedianStdDevDriver.class);
		job.setMapperClass(SOMedianStdDevMapper.class);
		job.setCombinerClass(SOMedianStdDevReducer.class);
		job.setReducerClass(SOMedianStdDevReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
