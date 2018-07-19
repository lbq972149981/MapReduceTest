package MP;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class globalSortExampleTest {
	public static class globalSortDateMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			String strDate = strs[1];
			outkey.set(strDate);

			context.write(outkey,new Text(""));
		}
	}
	public static class globalSortReducer extends Reducer<Text, Text, Text, NullWritable> {

		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MP.globalSortExampleTest");
		job.setJarByClass(globalSortExampleTest.class);
		job.setMapperClass(globalSortDateMapper.class);
		job.setReducerClass(globalSortReducer.class);
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
