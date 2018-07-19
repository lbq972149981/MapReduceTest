package TEST;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.tika.Tika;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
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
//      System.setProperty("hadoop.home.dir","D:\\hadoop2.7.6\\hadoop-2.7.6");
      Configuration conf = new Configuration();
      Job job = new Job(conf, "word count");
      job.setJarByClass(WordCount.class);
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