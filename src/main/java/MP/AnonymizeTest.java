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
import java.util.Map;
import java.util.Random;

public class AnonymizeTest {
   public static class AnonymizeMapper extends Mapper<Object,Text,IntWritable,Text>{
      private IntWritable outkey =  new IntWritable();
      private Random random = new Random();
      private Text outvalue = new Text();

      @Override
      protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
         if(parsed.size()>0){
            StringBuilder builder = new StringBuilder();
            builder.append("<row ");
            for(Map.Entry<String,String> entry:parsed.entrySet()){
               if(entry.getKey().equals("UserId")||entry.getKey().equals("Id")){
               }else if (entry.getKey().equals("CreationDate")){
                  builder.append(entry.getKey()+"=\""+entry.getValue().substring(0,entry.getValue().indexOf('T'))+"\"");
               }else {
                  builder.append(entry.getKey()+"=\""+entry.getValue()+"\"");
               }
            }
            builder.append("/>");
            outkey.set(random.nextInt());
            outvalue.set(builder.toString());
            context.write(outkey,outvalue);
         }
      }
   }
   public static class ValueReducer extends Reducer<IntWritable,Text,Text,NullWritable>{
      @Override
      protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         for(Text t:values){
            context.write(t,NullWritable.get());
         }
      }
   }
   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "MP.AnonymizeTest");
      job.setJarByClass(AnonymizeTest.class);
      job.setMapperClass(AnonymizeMapper.class);
      job.setReducerClass(ValueReducer.class);

      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      String inputPath = "input/Comments.xml";
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
