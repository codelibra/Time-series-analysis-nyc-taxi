import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class AnnualZScoreCalculatorReducer extends Reducer<Text, Text, Text, Text> {

 public static final double THRESHOLD = 10;

 public class ObjectComparator implements Comparator{

    public int compare(Object obj1, Object obj2) {
        String str1 = (String)obj1;
        String str2 = (String)obj2;
        Double residual1 = Double.parseDouble(str1.split(",")[4]);
        Double residual2 = Double.parseDouble(str2.split(",")[4]);
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
     ArrayList<Double> originalFreq = new ArrayList<Double>();
     for(String value : valuesList){
	
	String[] splitValues = value.toString().split(",");

        //if(!splitValues[0].contains("54907")) return;

        groupIds.add(splitValues[0]);
        longitude = splitValues[1];
	latitude  = splitValues[2];
	String residual = splitValues[4];
        residuals.add(Double.parseDouble(residual));
        originalFreq.add(Double.parseDouble(splitValues[3]));
     }

     if(residuals.size()<=2)
     {
              System.out.println("Gotcha " + residuals.size());
              return;
     }
	
     System.out.println("Residuals input to calculate median ");
     for(int i=0;i<residuals.size();i++)
	System.out.println(residuals.get(i));

     double y_median = getMedian(residuals);
     System.out.println("Median obtained is " + y_median);

     ArrayList<Double> abs_deviation_from_median = new ArrayList<Double>();
     for(int i=0;i<residuals.size();i++)
	abs_deviation_from_median.add(Math.abs(residuals.get(i)-y_median));
     
     System.out.println("Absolute Deviation from median is ");
     for(int i=0;i<abs_deviation_from_median.size();i++)
        System.out.println(abs_deviation_from_median.get(i));
     
     Collections.sort(abs_deviation_from_median);
     double median_of_abs_deviation_from_median = getMedian(abs_deviation_from_median);
     System.out.println("abs deviation median is " + median_of_abs_deviation_from_median);

     ArrayList<Double> modified_z_scores = new ArrayList<Double>();
     for(int i=0;i<residuals.size();i++)
	modified_z_scores.add( (0.6745 * Math.abs(residuals.get(i) - y_median))/median_of_abs_deviation_from_median);

     System.out.println("Z-scores are");
     for(int i=0;i<modified_z_scores.size();i++)
	System.out.println(groupIds.get(i)+" " + modified_z_scores.get(i));

     for(int i=0;i<modified_z_scores.size();i++)
     {
	  if(modified_z_scores.get(i)>=THRESHOLD)
	  {
 	         String output = longitude + "," + latitude + "," + originalFreq.get(i) + "," + modified_z_scores.get(i);
        	 context.write(new Text(groupIds.get(i)), new Text(output));
	  }
     }
 }
}
