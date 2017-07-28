import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnnualZScoreCalculatorMapper extends Mapper<LongWritable, Text, Text,Text> {
 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

   String[] inputLine = value.toString().split("\t");
   
   String[]  splitValues = inputLine[1].split(",");
   
   String[] splitKey = splitValues[0].split("_");
   
   String newKey = splitKey[0] + "_" + splitKey[2]; 
 
  
   String outputValue = splitValues[0] + "," + splitValues[1] + "," + splitValues[2] + "," + splitValues[3] + "," + splitValues[6]; 


   context.write(new Text(newKey), new Text(outputValue));
 }
}

