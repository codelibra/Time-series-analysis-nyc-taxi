import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text,Text> {
 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

   String[] inputLine = value.toString().split(",");
   /**
    * Output will be generated in the following format
    * pickUpTimestamp, DropOffTimestamp, pickUpLongitude, pickUpLatitude, dropOffLongitude, dropOffLatitide
    **/
try{
        Double pickUpLong   = Double.parseDouble(inputLine[5]);
        Double pickUpLat    = Double.parseDouble(inputLine[6]);
        Double dropOffLong  = Double.parseDouble(inputLine[9]);
        Double dropOffLat   = Double.parseDouble(inputLine[10]);

   	if(pickUpLong==0.0 || pickUpLat==0.0 || dropOffLong==0.0 || dropOffLat==0.0 || (pickUpLong==dropOffLong && pickUpLat==dropOffLat)){
             System.out.println("Removing line number "+ value);
   	}
        else{
           String output = inputLine[1] +","+inputLine[2]+","+pickUpLong+"," + pickUpLat+"," + dropOffLong + "," + dropOffLat;
           context.write(new Text(""), new Text(output));
	}
}
catch(Exception e){
System.out.println(e);
}
 }
}

