package TEST;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyFileOutputFormat extends FileOutputFormat<Text,Text>{
   @Override
   public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
      FileSystem fs = FileSystem.newInstance(job.getConfiguration());
      final FSDataOutputStream math = fs.create(new Path("output/wordtest.txt"));
      RecordWriter<Text, Text> recordWriter = new RecordWriter<Text, Text>() {
         @Override
         public void write(Text key, Text value) throws IOException, InterruptedException {
            if(key.toString().contains("wordtest")){
               math.writeUTF(key.toString());
            }
         }

         @Override
         public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (math!=null) {
               math.close();
            }
         }
      };
      return recordWriter;
   }
}