package reverseIndexTest1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Music {
	public static class MusicMap extends Mapper<Object, Text, Text, Text> {
        //private Text userName = new Text();
        //private Text musicName = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
               System.out.println(value.toString());
            while (itr.hasMoreTokens()) {
                //tom,LittleApple
                //jack,YesterdayOnceMore
            		
                String content = itr.nextToken();
                String[] splits = content.split(",");
                String name = splits[0];
                String music = splits[1];
                context.write(new Text(music), new Text(name));
            }
        }
    }

    public static class MusicReduce extends Reducer<Text, Text, Text, Text> {
        private Text userNames = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            userNames.set("");
            StringBuffer result = new StringBuffer();
            int i = 0;
            for (Text tempText : values) {
                result.append(tempText.toString()+"  ");
                i++;
            }
            userNames.set(result.toString());
            context.write(key, userNames);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MinMaxCountDriver <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "StackOverflow Comment Date Min Max Count");
        job.setJarByClass(Music.class);
        job.setMapperClass(MusicMap.class);
        //job.setCombinerClass(MusicReduce.class);
        job.setReducerClass(MusicReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
