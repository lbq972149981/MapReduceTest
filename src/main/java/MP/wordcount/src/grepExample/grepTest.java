package grepExample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class grepTest {
	public static class GrepMapper extends Mapper<Object, Text, Text, Text>{
		private String mapRegex = null;
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mapRegex = context.getConfiguration().get("mapregex");
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] strs = value.toString().split(" ");
			int count = 0;
			for(String v:strs){
				if(v.toString().matches(mapRegex)){
					Text t = new Text(String.valueOf(count));
					context.write(t,new Text(v));
					count++;
				}
			}
			
		}
	}
    public static void main(String[] args) throws Exception {    
        Configuration conf = new Configuration(); 
        	conf.set("mapregex","^[0-9]*[1-9][0-9]*$");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();    
        if (otherArgs.length < 2) {    
            System.err.println("Usage: EventCount <in> <out>");    
            System.exit(2);    
        }    
        Job job = Job.getInstance(conf, "grep compute");    
        job.setJarByClass(grepTest.class);    
        job.setMapperClass(GrepMapper.class);        
        job.setOutputKeyClass(Text.class);    
        job.setOutputValueClass(Text.class);    
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));    
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));    
        System.exit(job.waitForCompletion(true) ? 0 : 1);    
    }

}
