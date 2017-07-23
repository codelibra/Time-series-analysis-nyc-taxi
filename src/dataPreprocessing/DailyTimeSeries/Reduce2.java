import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {
 
 @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
  int length = 0; 
  String input="";
  for(Text v : values){
     input = v.toString();
     ++length;
  }
  input = input + "," + Integer.toString(length);
  context.write(key, new Text(input));
 }
}
