package CountNumber;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountNumUsersByStateMapper extends Mapper<Object,Text,NullWritable,NullWritable>{
   public static final String STATE_COUNTER_GROUP = "State";
   public static final String UNKNOWN_COUNTER = "Unknown";
   public static final String NULL_OR_EMPTY_COUNTER ="Null or Empty";
   private String[] statesArray = new String[]{
     "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
         "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH",
         "OK","OR","PA","RI","SC","SF","TN","TX","UT","VT","VA","WA","WV","WI","WY"
   };
   private HashSet<String> states = new HashSet<>(Arrays.asList(statesArray));
   public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
      Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
      String location = parsed.get("Location");
      if(location !=null&&!location.isEmpty()){
         String[] tokens = location.toUpperCase().split("\\s");
         boolean unknown = true;
         for(String state:tokens){
            if(states.contains(state)){
               context.getCounter(STATE_COUNTER_GROUP,state).increment(1);
               unknown = false;
               break;
            }
         }
         if(unknown){
            context.getCounter(STATE_COUNTER_GROUP,UNKNOWN_COUNTER).increment(1);
         }
      }else {
         context.getCounter(STATE_COUNTER_GROUP,NULL_OR_EMPTY_COUNTER).increment(1);
      }
   }
}
