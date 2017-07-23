import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GridAppenderReducer extends Reducer<Text, Text, Text, Text> 
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
        // Reducer just spits off input
        Text emptyString = new Text(""); 
	for(Text value : values){
            context.write(emptyString, value);
        }
    }
}
