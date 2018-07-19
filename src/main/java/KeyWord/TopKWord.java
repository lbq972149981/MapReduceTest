package KeyWord;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class TopKWord {
	public static class TopKMapper extends Mapper<Object, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
				String arr[] = value.toString().split("\\s+");
				repToRecordMap.put(Integer.parseInt(arr[1]), new Text(value));
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
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

	public static class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<>();

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text value : values) {
				String arr[] = value.toString().split("\\s+");
				repToRecordMap.put(Integer.parseInt(arr[1]), new Text(value));
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			for(Text t:repToRecordMap.descendingMap().values()){
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken()+" ");
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
								 Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(new Text(key.toString()+" "), result);
		}
	}
	public static void main(String[] args) throws Exception {
		KeyWordCount();
	}
	public static void TopKKeyword() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Key word");
		job.setJarByClass(TopKWord.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		String inputPath = "input/temp.txt";
		String outputPath = "output/";
		FileInputFormat.addInputPath(job, new Path(inputPath));
		// 判断输出文件是否存在
		Path output = new Path(outputPath);
		FileSystem fileSystem = output.getFileSystem(conf);
		if(fileSystem.exists(output)){
			fileSystem.delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static void KeyWordCount() throws IOException, TikaException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");
		job.setJarByClass(TopKWord.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		String inputPath = "input/temp.txt";
		String outputPath = "output/";

		Tika tika = new Tika();
		String read = tika.parseToString(new File("input/tt.docx"));//待输入文本
		Segment segment = HanLP.newSegment();
		FileWriter fileWriter = new FileWriter("input/temp.txt");//转换为temp.txt
		for(Term term:segment.seg(read)){
			String s[] = term.toString().split("/");
			String key = s[0];
			String type = s[1];
			if(type.equals("n")) {
				fileWriter.write(key + "\n");
				fileWriter.flush();
			}
		}
		fileWriter.close();
		MultipleOutputs.addNamedOutput(job,"topk", TextOutputFormat.class,Text.class,IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		// 判断输出文件是否存在
		Path output = new Path(outputPath);
		FileSystem fileSystem = output.getFileSystem(conf);
		if(fileSystem.exists(output)){
			fileSystem.delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
}