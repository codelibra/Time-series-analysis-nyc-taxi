import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {

 public static final double THRESHOLD = 10;

 public class ObjectComparator implements Comparator{

    public int compare(Object obj1, Object obj2) {
        String str1 = (String)obj1;
        String str2 = (String)obj2;
        Double residual1 = Double.parseDouble(str1.split(",")[3]);
        Double residual2 = Double.parseDouble(str2.split(",")[3]);
        return residual1.compareTo(residual2);
   }
 }
 
 public double getMedian(ArrayList<Double> arr)
 {
	if(arr.size()%2!=0)
		return arr.get(arr.size()/2);
	
	return (arr.get(arr.size()/2) + arr.get((arr.size()/2)+1))/2.0;
 }

 @Override
 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  

     ArrayList<String> valuesList = new ArrayList<String>();
     for(Text value : values){
          valuesList.add(value.toString());
     } 
     Collections.sort(valuesList, new ObjectComparator());     

     String longitude="",latitude="";
     ArrayList<Double> residuals = new ArrayList<Double>();
     ArrayList<String> groupIds = new ArrayList<String>();
     for(String value : valuesList){
	
	String[] splitValues = value.toString().split(",");
        groupIds.add(splitValues[0]);
        longitude = splitValues[1];
	latitude  = splitValues[2];
	String residual = splitValues[3];
        residuals.add(Double.parseDouble(residual));
     }

     if(residuals.size()<=2)
     {
              System.out.println("Gotcha " + residuals.size());
              return;
     }

     double y_median = getMedian(residuals);
     ArrayList<Double> abs_deviation_from_median = new ArrayList<Double>();
     for(int i=0;i<residuals.size();i++)
	abs_deviation_from_median.add(Math.abs(residuals.get(i)-y_median));
     double median_of_abs_deviation_from_median = getMedian(abs_deviation_from_median);
 
     ArrayList<Double> modified_z_scores = new ArrayList<Double>();
     for(int i=0;i<residuals.size();i++)
	modified_z_scores.add( (0.6745 * Math.abs(residuals.get(i) - y_median))/median_of_abs_deviation_from_median);

     for(int i=0;i<modified_z_scores.size();i++)
     {
	  if(modified_z_scores.get(i)>=THRESHOLD)
	  {
 	         String output = latitude + "," + longitude;
        	 context.write(new Text(groupIds.get(i)), new Text(output));
	  }
     }
 }
}
