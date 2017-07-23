import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, Text,Text> {
 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

   String[] inputLine = value.toString().split(",");
   String day = inputLine[0].split(" ")[0];
   String dayAndpickUpGridId = day+"_"+inputLine[6];
   String dayAnddropOffGridId = day+"_"+inputLine[9];
   
   String pickUpOutput = inputLine[6]+","+ inputLine[7]+","+inputLine[8];
   String dropOffOutput = inputLine[9]+","+ inputLine[10]+","+inputLine[11];

   context.write(new Text(dayAndpickUpGridId), new Text(pickUpOutput));
   context.write(new Text(dayAnddropOffGridId), new Text(dropOffOutput));
 }
}

