package reverseIndexTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexRunner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(InvertedIndexRunner.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 检查参数所指定的输出路径是否存在，若存在，先删除
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		FileOutputFormat.setOutputPath(job, output);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
