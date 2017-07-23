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
   try{
        String pickUpTimeStamp  = inputLine[1]; 
        String dropOffTimeStamp = inputLine[2];
         
        String pickUpLong  = inputLine[5]; 
        String pickUpLat = inputLine[6];

        String dropOffLong  = inputLine[9]; 
        String dropOffLat = inputLine[10];

   	if(pickUpTimeStamp.length()==0 || dropOffTimeStamp.length()==0 || pickUpLong.length()==0 || pickUpLat.length()==0
           || dropOffLong.length()==0 || dropOffLat.length()==0){
             System.out.println("Removing line number "+ key.toString());
   	}
        else{
           String output = day +","+pickUpLong+"," + pickUpLat+"," + dropOffLong + "," + dropOffLat;
           context.write(new Text(day), new Text(output));
	}
   }
   catch(Exception e){
   }
 }
}

