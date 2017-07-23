import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Map2 extends Mapper<LongWritable, Text, Text,Text> {
 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  try{

   String[] inputLine = value.toString().split(",");
   String pickUpDate = inputLine[0];
   String dropOffDate = inputLine[1];

   String format = "yyyy-MM-dd";
   SimpleDateFormat df = new SimpleDateFormat(format);
   Date pDate = df.parse(pickUpDate);
   Date dDate = df.parse(dropOffDate);

   Calendar cal = Calendar.getInstance();
   cal.setTime(pDate);
   int pickUpWeek = cal.get(Calendar.WEEK_OF_YEAR);
   cal.setTime(dDate);
   int dropOffWeek = cal.get(Calendar.WEEK_OF_YEAR);

   String weekAndYearPickUp = Integer.toString(pickUpWeek) +"_"+ cal.get(Calendar.YEAR);
   String weekAndYearDropOff =  Integer.toString(dropOffWeek) +"_"+ cal.get(Calendar.YEAR);
   String dayAndpickUpGridId = weekAndYearPickUp+"_"+inputLine[6];
   String dayAnddropOffGridId = weekAndYearDropOff+"_"+inputLine[9];
   
   String pickUpOutput = inputLine[6]+","+ inputLine[7]+","+inputLine[8];
   String dropOffOutput = inputLine[9]+","+ inputLine[10]+","+inputLine[11];

   context.write(new Text(dayAndpickUpGridId), new Text(pickUpOutput));
   context.write(new Text(dayAnddropOffGridId), new Text(dropOffOutput));
}
catch(Exception e){}
 }
}

