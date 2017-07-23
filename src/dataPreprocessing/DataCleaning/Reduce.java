import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
 
 @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
  
  for(Text v : values){
     String value = v.toString();
     context.write(new Text(""), new Text(value));
  }
 }
}
